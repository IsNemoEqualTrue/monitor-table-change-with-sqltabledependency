using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient.BaseTests;

namespace TableDependency.SqlClient.IntegrationTests
{
    public class CultureInfoTestModel
    {
        public string Name { get; set; }

        public DateTime BirthDate { get; set; }
    }

    [TestClass]
    public class CultureInfoTest : SqlTableDependencyBaseTest
    {
        private static readonly string TableName1 = typeof(CultureInfoTestModel).Name + "1";
        private static readonly string TableName2 = typeof(CultureInfoTestModel).Name + "2";
        private static int _counter1;
        private static int _counter2;
        private static readonly Dictionary<string, CultureInfoTestModel> CheckValues1 = new Dictionary<string, CultureInfoTestModel>();
        private static readonly Dictionary<string, CultureInfoTestModel> CheckValues2 = new Dictionary<string, CultureInfoTestModel>();

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName1}', 'U') IS NOT NULL DROP TABLE [{TableName1}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName1}]([Name] [NVARCHAR](50) NULL, [BirthDate] [DATETIME] NULL)";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName2}', 'U') IS NOT NULL DROP TABLE [{TableName2}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName2}]([Name] [NVARCHAR](50) NULL, [BirthDate] [DATETIME] NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize]
        public void TestInitialize()
        {
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName1}', 'U') IS NOT NULL DROP TABLE [{TableName1}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName2}', 'U') IS NOT NULL DROP TABLE [{TableName2}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void Test1()
        {
            SqlTableDependency<CultureInfoTestModel> tableDependency = null;
            string naming;

            CheckValues1.Add(ChangeType.Insert.ToString(), new CultureInfoTestModel());
            CheckValues1.Add(ChangeType.Update.ToString(), new CultureInfoTestModel());
            CheckValues1.Add(ChangeType.Delete.ToString(), new CultureInfoTestModel());

            try
            {
                tableDependency = new SqlTableDependency<CultureInfoTestModel>(ConnectionStringForTestUser, tableName: TableName1);
                naming = tableDependency.DataBaseObjectsNamingConvention;
                tableDependency.OnChanged += TableDependency_Changed1;
                tableDependency.CultureInfo = new CultureInfo("it-IT");

                tableDependency.Start();
                var t = new Task(ModifyTableContent1);
                t.Start();
                Thread.Sleep(1000 * 5 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter1, 3);
           
            Assert.AreEqual("Christian", CheckValues1[ChangeType.Insert.ToString()].Name);
            Assert.AreEqual(DateTime.ParseExact("2009-08-05", "yyyy-MM-dd", new CultureInfo("it-IT")), CheckValues1[ChangeType.Insert.ToString()].BirthDate);

            Assert.AreEqual("Valentina", CheckValues1[ChangeType.Update.ToString()].Name);
            Assert.AreEqual(DateTime.ParseExact("2009-05-08", "yyyy-MM-dd", new CultureInfo("it-IT")), CheckValues1[ChangeType.Update.ToString()].BirthDate);
            
            Assert.AreEqual("Valentina", CheckValues1[ChangeType.Delete.ToString()].Name);
            Assert.AreEqual(DateTime.ParseExact("2009-05-08", "yyyy-MM-dd", new CultureInfo("it-IT")), CheckValues1[ChangeType.Delete.ToString()].BirthDate);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void Test2()
        {
            SqlTableDependency<CultureInfoTestModel> tableDependency = null;
            string naming;

            CheckValues2.Add(ChangeType.Insert.ToString(), new CultureInfoTestModel());
            CheckValues2.Add(ChangeType.Update.ToString(), new CultureInfoTestModel());
            CheckValues2.Add(ChangeType.Delete.ToString(), new CultureInfoTestModel());

            try
            {
                tableDependency = new SqlTableDependency<CultureInfoTestModel>(ConnectionStringForTestUser, tableName: TableName2);
                naming = tableDependency.DataBaseObjectsNamingConvention;
                tableDependency.OnChanged += TableDependency_Changed2;
                tableDependency.CultureInfo = new CultureInfo("en-US");

                tableDependency.Start();
                var t = new Task(ModifyTableContent2);
                t.Start();
                Thread.Sleep(1000 * 5 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter2, 3);

            Assert.AreEqual("Christian", CheckValues2[ChangeType.Insert.ToString()].Name);
            Assert.AreEqual(DateTime.ParseExact("2009-08-05", "yyyy-MM-dd", new CultureInfo("en-US")), CheckValues2[ChangeType.Insert.ToString()].BirthDate);

            Assert.AreEqual("Valentina", CheckValues2[ChangeType.Update.ToString()].Name);
            Assert.AreEqual(DateTime.ParseExact("2009-05-08", "yyyy-MM-dd", new CultureInfo("en-US")), CheckValues2[ChangeType.Update.ToString()].BirthDate);

            Assert.AreEqual("Valentina", CheckValues2[ChangeType.Delete.ToString()].Name);
            Assert.AreEqual(DateTime.ParseExact("2009-05-08", "yyyy-MM-dd", new CultureInfo("en-US")), CheckValues2[ChangeType.Delete.ToString()].BirthDate);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private static void TableDependency_Changed1(object sender, RecordChangedEventArgs<CultureInfoTestModel> e)
        {
            _counter1++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues1[ChangeType.Insert.ToString()].Name = e.Entity.Name;
                    CheckValues1[ChangeType.Insert.ToString()].BirthDate = e.Entity.BirthDate;
                    break;
                case ChangeType.Update:
                    CheckValues1[ChangeType.Update.ToString()].Name = e.Entity.Name;
                    CheckValues1[ChangeType.Update.ToString()].BirthDate = e.Entity.BirthDate;
                    break;
                case ChangeType.Delete:
                    CheckValues1[ChangeType.Delete.ToString()].Name = e.Entity.Name;
                    CheckValues1[ChangeType.Delete.ToString()].BirthDate = e.Entity.BirthDate;
                    break;
            }
        }

        private static void TableDependency_Changed2(object sender, RecordChangedEventArgs<CultureInfoTestModel> e)
        {
            _counter2++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues2[ChangeType.Insert.ToString()].Name = e.Entity.Name;
                    CheckValues2[ChangeType.Insert.ToString()].BirthDate = e.Entity.BirthDate;
                    break;
                case ChangeType.Update:
                    CheckValues2[ChangeType.Update.ToString()].Name = e.Entity.Name;
                    CheckValues2[ChangeType.Update.ToString()].BirthDate = e.Entity.BirthDate;
                    break;
                case ChangeType.Delete:
                    CheckValues2[ChangeType.Delete.ToString()].Name = e.Entity.Name;
                    CheckValues2[ChangeType.Delete.ToString()].BirthDate = e.Entity.BirthDate;
                    break;
            }
        }

        private static void ModifyTableContent1()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName1}] ([Name], [BirthDate]) VALUES ('Christian', '2009-08-05')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName1}] SET [Name] = 'Valentina', [BirthDate] = '2009-05-08'";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName1}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        private static void ModifyTableContent2()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName2}] ([Name], [BirthDate]) VALUES ('Christian', '2009-08-05')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName2}] SET [Name] = 'Valentina', [BirthDate] = '2009-05-08'";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName2}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}