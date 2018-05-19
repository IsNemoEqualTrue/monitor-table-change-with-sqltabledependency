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
using TableDependency.SqlClient.BaseTests;

namespace TableDependency.SqlClient.IntegrationTests
{
    [TestClass]
    public class ComputedColumnTest : SqlTableDependencyBaseTest
    {
        private class ComputedColumnModel
        {
            public string Name { get; set; }

            public DateTime BirthDate { get; set; }

            [Column("Age")]
            public int CalculatedAge { get; set; }
        }

        private static readonly string TableName = typeof(ComputedColumnModel).Name;
        private static int _counter;
        private static readonly Dictionary<string, Tuple<ComputedColumnModel, ComputedColumnModel>> CheckValues = new Dictionary<string, Tuple<ComputedColumnModel, ComputedColumnModel>>();
        private static readonly Dictionary<string, Tuple<ComputedColumnModel, ComputedColumnModel>> CheckValuesOld = new Dictionary<string, Tuple<ComputedColumnModel, ComputedColumnModel>>();

        [ClassInitialize]
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

        [TestInitialize]
        public void TestInitialize()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }

            CheckValues.Clear();
            CheckValuesOld.Clear();

            _counter = 0;

            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<ComputedColumnModel, ComputedColumnModel>(new ComputedColumnModel { Name = "Christian", BirthDate = DateTime.Now.AddYears(-46).Date, CalculatedAge = 46 }, new ComputedColumnModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<ComputedColumnModel, ComputedColumnModel>(new ComputedColumnModel { Name = "Nonna Velia", BirthDate = DateTime.Now.AddYears(-95).Date, CalculatedAge = 95 }, new ComputedColumnModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<ComputedColumnModel, ComputedColumnModel>(new ComputedColumnModel { Name = "Nonna Velia", BirthDate = DateTime.Now.AddYears(-95).Date, CalculatedAge = 95 }, new ComputedColumnModel()));

            CheckValuesOld.Add(ChangeType.Insert.ToString(), new Tuple<ComputedColumnModel, ComputedColumnModel>(new ComputedColumnModel { Name = "Christian", BirthDate = DateTime.Now.AddYears(-46).Date, CalculatedAge = 46 }, new ComputedColumnModel()));
            CheckValuesOld.Add(ChangeType.Update.ToString(), new Tuple<ComputedColumnModel, ComputedColumnModel>(new ComputedColumnModel { Name = "Nonna Velia", BirthDate = DateTime.Now.AddYears(-95).Date, CalculatedAge = 95 }, new ComputedColumnModel()));
            CheckValuesOld.Add(ChangeType.Delete.ToString(), new Tuple<ComputedColumnModel, ComputedColumnModel>(new ComputedColumnModel { Name = "Nonna Velia", BirthDate = DateTime.Now.AddYears(-95).Date, CalculatedAge = 95 }, new ComputedColumnModel()));
        }

        [ClassCleanup]
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
        public void TestWithOldValues()
        {
            SqlTableDependency<ComputedColumnModel> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<ComputedColumnModel>(ConnectionStringForTestUser, includeOldValues: true);
                naming = tableDependency.DataBaseObjectsNamingConvention;
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.BirthDate, CheckValues[ChangeType.Insert.ToString()].Item1.BirthDate);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.CalculatedAge, CheckValues[ChangeType.Insert.ToString()].Item1.CalculatedAge);
            Assert.IsNull(CheckValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, CheckValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.BirthDate, CheckValues[ChangeType.Update.ToString()].Item1.BirthDate);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.CalculatedAge, CheckValues[ChangeType.Update.ToString()].Item1.CalculatedAge);
            Assert.AreEqual(CheckValuesOld[ChangeType.Update.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item2.Name);
            Assert.AreEqual(CheckValuesOld[ChangeType.Update.ToString()].Item2.BirthDate, CheckValues[ChangeType.Insert.ToString()].Item2.BirthDate);
            Assert.AreEqual(CheckValuesOld[ChangeType.Update.ToString()].Item2.CalculatedAge, CheckValues[ChangeType.Insert.ToString()].Item2.CalculatedAge);
            
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.BirthDate, CheckValues[ChangeType.Delete.ToString()].Item1.BirthDate);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.CalculatedAge, CheckValues[ChangeType.Delete.ToString()].Item1.CalculatedAge);
            Assert.IsNull(CheckValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
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
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.BirthDate, CheckValues[ChangeType.Insert.ToString()].Item1.BirthDate);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.CalculatedAge, CheckValues[ChangeType.Insert.ToString()].Item1.CalculatedAge);
            Assert.IsNull(CheckValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, CheckValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.BirthDate, CheckValues[ChangeType.Update.ToString()].Item1.BirthDate);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.CalculatedAge, CheckValues[ChangeType.Update.ToString()].Item1.CalculatedAge);
            Assert.IsNull(CheckValuesOld[ChangeType.Update.ToString()]);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.BirthDate, CheckValues[ChangeType.Delete.ToString()].Item1.BirthDate);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.CalculatedAge, CheckValues[ChangeType.Delete.ToString()].Item1.CalculatedAge);
            Assert.IsNull(CheckValuesOld[ChangeType.Delete.ToString()]);

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

                    if (e.EntityOldValues != null)
                    {
                        CheckValuesOld[ChangeType.Insert.ToString()].Item2.Name = e.EntityOldValues.Name;
                        CheckValuesOld[ChangeType.Insert.ToString()].Item2.BirthDate = e.EntityOldValues.BirthDate;
                        CheckValuesOld[ChangeType.Insert.ToString()].Item2.CalculatedAge = e.EntityOldValues.CalculatedAge;
                    }
                    else
                    {
                        CheckValuesOld[ChangeType.Insert.ToString()] = null;
                    }

                    break;

                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Update.ToString()].Item2.BirthDate = e.Entity.BirthDate;
                    CheckValues[ChangeType.Update.ToString()].Item2.CalculatedAge = e.Entity.CalculatedAge;

                    if (e.EntityOldValues != null)
                    {
                        CheckValuesOld[ChangeType.Update.ToString()].Item2.Name = e.EntityOldValues.Name;
                        CheckValuesOld[ChangeType.Update.ToString()].Item2.BirthDate = e.EntityOldValues.BirthDate;
                        CheckValuesOld[ChangeType.Update.ToString()].Item2.CalculatedAge = e.EntityOldValues.CalculatedAge;
                    }
                    else
                    {
                        CheckValuesOld[ChangeType.Update.ToString()] = null;
                    }

                    break; 

                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Delete.ToString()].Item2.BirthDate = e.Entity.BirthDate;
                    CheckValues[ChangeType.Delete.ToString()].Item2.CalculatedAge = e.Entity.CalculatedAge;

                    if (e.EntityOldValues != null)
                    {
                        CheckValuesOld[ChangeType.Delete.ToString()].Item2.Name = e.EntityOldValues.Name;
                        CheckValuesOld[ChangeType.Delete.ToString()].Item2.BirthDate = e.EntityOldValues.BirthDate;
                        CheckValuesOld[ChangeType.Delete.ToString()].Item2.CalculatedAge = e.EntityOldValues.CalculatedAge;
                    }
                    else
                    {
                        CheckValuesOld[ChangeType.Delete.ToString()] = null;
                    }

                    break;
            }
        }

        private static void ModifyTableContent()
        {            
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