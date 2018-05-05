using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Base;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    public class CultureInfoTestModel
    {
        public string Name { get; set; }

        public DateTime BirthDate { get; set; }
    }

    [TestClass]
    public class CultureInfoTest : SqlTableDependencyBaseTest
    {
        private static DateTime _dt1;
        private static DateTime _dt2;
        private static DateTime _dt3;
        private static readonly string TableName = typeof(CultureInfoTestModel).Name;
        private static int _counter;
        private static readonly Dictionary<string, CultureInfoTestModel> CheckValues = new Dictionary<string, CultureInfoTestModel>();

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
                }
            }
        }

        [TestInitialize()]
        public void TestInitialize()
        {
            _dt1 = DateTime.ParseExact("2009-05-08", "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);
            _dt2 = DateTime.ParseExact("2009-08-05", "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);
            _dt3 = DateTime.ParseExact("2009-08-05", "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);

            CheckValues.Add(ChangeType.Insert.ToString(), new CultureInfoTestModel());
            CheckValues.Add(ChangeType.Update.ToString(), new CultureInfoTestModel());
            CheckValues.Add(ChangeType.Delete.ToString(), new CultureInfoTestModel());
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
            SqlTableDependency<CultureInfoTestModel> tableDependency = null;
            string naming;

            Thread.CurrentThread.CurrentCulture = new CultureInfo("it-IT");

            try
            {
                tableDependency = new SqlTableDependency<CultureInfoTestModel>(ConnectionStringForTestUser);
                naming = tableDependency.DataBaseObjectsNamingConvention;
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.CultureInfo = new CultureInfo("it-IT");

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
           
            Assert.AreEqual("Christian", CheckValues[ChangeType.Insert.ToString()].Name);
            Assert.AreEqual(_dt1, CheckValues[ChangeType.Insert.ToString()].BirthDate);

            Assert.AreEqual("Valentina", CheckValues[ChangeType.Update.ToString()].Name);
            Assert.AreEqual(_dt2, CheckValues[ChangeType.Update.ToString()].BirthDate);
            
            Assert.AreEqual("Valentina", CheckValues[ChangeType.Delete.ToString()].Name);
            Assert.AreEqual(_dt3, CheckValues[ChangeType.Delete.ToString()].BirthDate);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<CultureInfoTestModel> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Name = e.Entity.Name;
                    CheckValues[ChangeType.Insert.ToString()].BirthDate = e.Entity.BirthDate;
                    break;
                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Name = e.Entity.Name;
                    CheckValues[ChangeType.Update.ToString()].BirthDate = e.Entity.BirthDate;
                    break;
                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Name = e.Entity.Name;
                    CheckValues[ChangeType.Delete.ToString()].BirthDate = e.Entity.BirthDate;
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
                    sqlCommand.Parameters.Add(new SqlParameter("@name", SqlDbType.VarChar) { Value = "Christian" });
                    sqlCommand.Parameters.Add(new SqlParameter("@birth", SqlDbType.Date) { Value = _dt1 });
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = @name, [BirthDate] = @birth";
                    sqlCommand.Parameters.Add(new SqlParameter("@name", SqlDbType.VarChar) { Value = "Valentina" });
                    sqlCommand.Parameters.Add(new SqlParameter("@birth", SqlDbType.Date) { Value = _dt2 });
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