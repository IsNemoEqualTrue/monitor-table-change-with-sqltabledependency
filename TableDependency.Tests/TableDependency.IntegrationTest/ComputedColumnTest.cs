using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Exceptions;
using TableDependency.IntegrationTest.Base;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    public class ComputedColumnModel
    {
        public string Name { get; set; }

        public DateTime BirthDate { get; set; }

        [Column("Age")]
        public int CalculatedAge { get; set; }
    }

    [TestClass]
    public class ComputedColumnTest : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(ComputedColumnModel).Name;
        private static int _counter;
        private static readonly Dictionary<string, Tuple<ComputedColumnModel, ComputedColumnModel>> CheckValues = new Dictionary<string, Tuple<ComputedColumnModel, ComputedColumnModel>>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Name] [NVARCHAR](50) NULL, [BirthDate] [DATETIME] NULL)";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"ALTER TABLE [{TableName}] ADD [Age] AS DATEDIFF(YEAR, [BirthDate], GETDATE())";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize()]
        public void TestInitialize()
        {
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void Test()
        {
            SqlTableDependency<ComputedColumnModel> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<ComputedColumnModel>(ConnectionStringForTestUser);
                naming = tableDependency.DataBaseObjectsNamingConvention;
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 5 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.BirthDate, CheckValues[ChangeType.Insert.ToString()].Item1.BirthDate);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.CalculatedAge, CheckValues[ChangeType.Insert.ToString()].Item1.CalculatedAge);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, CheckValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.BirthDate, CheckValues[ChangeType.Update.ToString()].Item1.BirthDate);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.CalculatedAge, CheckValues[ChangeType.Update.ToString()].Item1.CalculatedAge);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.BirthDate, CheckValues[ChangeType.Delete.ToString()].Item1.BirthDate);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.CalculatedAge, CheckValues[ChangeType.Delete.ToString()].Item1.CalculatedAge);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<ComputedColumnModel> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Insert.ToString()].Item2.BirthDate = e.Entity.BirthDate;
                    CheckValues[ChangeType.Insert.ToString()].Item2.CalculatedAge = e.Entity.CalculatedAge;
                    break;
                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Update.ToString()].Item2.BirthDate = e.Entity.BirthDate;
                    CheckValues[ChangeType.Update.ToString()].Item2.CalculatedAge = e.Entity.CalculatedAge;
                    break;
                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Delete.ToString()].Item2.BirthDate = e.Entity.BirthDate;
                    CheckValues[ChangeType.Delete.ToString()].Item2.CalculatedAge = e.Entity.CalculatedAge;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<ComputedColumnModel, ComputedColumnModel>(new ComputedColumnModel { Name = "Christian", BirthDate = DateTime.Now.AddYears(-46).Date, CalculatedAge = 46 }, new ComputedColumnModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<ComputedColumnModel, ComputedColumnModel>(new ComputedColumnModel { Name = "Nonna Velia", BirthDate = DateTime.Now.AddYears(-95).Date, CalculatedAge = 95 }, new ComputedColumnModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<ComputedColumnModel, ComputedColumnModel>(new ComputedColumnModel { Name = "Nonna Velia", BirthDate = DateTime.Now.AddYears(-95).Date, CalculatedAge = 95 }, new ComputedColumnModel()));

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name], [BirthDate]) VALUES (@name, @birth)";
                    sqlCommand.Parameters.Add(new SqlParameter("@name", SqlDbType.VarChar) { Value = CheckValues[ChangeType.Insert.ToString()].Item1.Name });
                    sqlCommand.Parameters.Add(new SqlParameter("@birth", SqlDbType.Date) { Value = CheckValues[ChangeType.Insert.ToString()].Item1.BirthDate });
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = @name, [BirthDate] = @birth";
                    sqlCommand.Parameters.Add(new SqlParameter("@name", SqlDbType.VarChar) { Value = CheckValues[ChangeType.Update.ToString()].Item1.Name });
                    sqlCommand.Parameters.Add(new SqlParameter("@birth", SqlDbType.Date) { Value = CheckValues[ChangeType.Update.ToString()].Item1.BirthDate });
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}