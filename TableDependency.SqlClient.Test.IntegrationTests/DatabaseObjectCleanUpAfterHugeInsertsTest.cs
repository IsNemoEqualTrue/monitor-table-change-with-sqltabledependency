using System;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Test.Base;

namespace TableDependency.SqlClient.Test.IntegrationTests
{
    [TestClass]
    public class DatabaseObjectCleanUpAfterHugeInsertsTest : SqlTableDependencyBaseTest
    {
        private class DatabaseObjectCleanUpAfterHugeInsertsTestSqlServerModel
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public string Surname { get; set; }
            public DateTime Born { get; set; }
            public int Quantity { get; set; }
        }

        private static readonly string TableName = typeof(DatabaseObjectCleanUpAfterHugeInsertsTestSqlServerModel).Name;
        public static string DbObjectsNaming;

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

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id][int], [First Name] [nvarchar](50), [Second Name] [nvarchar](50))";
                    sqlCommand.ExecuteNonQuery();
                }
            }
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
        public void Test()
        {
            var mapper = new ModelToTableMapper<DatabaseObjectCleanUpAfterHugeInsertsTestSqlServerModel>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second Name");

            var tableDependency = new SqlTableDependency<DatabaseObjectCleanUpAfterHugeInsertsTestSqlServerModel>(
                ConnectionStringForTestUser, 
                includeOldValues: true, 
                tableName: TableName, 
                mapper: mapper);

            tableDependency.OnChanged += TableDependency_OnChanged;
            tableDependency.Start();
            DbObjectsNaming = tableDependency.DataBaseObjectsNamingConvention;

            var t = new Task(BigModifyTableContent);
            t.Start();
            Thread.Sleep(1000 * 15 * 1);

            Thread.Sleep(1000 * 30 * 1);
            tableDependency.Stop();

            SmalModifyTableContent();

            Thread.Sleep(4 * 60 * 1000);
            Assert.IsTrue(base.AreAllDbObjectDisposed(DbObjectsNaming));
            Assert.IsTrue(base.CountConversationEndpoints(DbObjectsNaming) == 0);
        }

        private static void BigModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    for (var i = 0; i < 100000; i++)
                    {
                        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{i}', '{i}')";
                        sqlCommand.ExecuteNonQuery();
                    }
                }
            }
        }

        private static void SmalModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('allora', 'mah')";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        private void TableDependency_OnChanged(object sender, TableDependency.EventArgs.RecordChangedEventArgs<DatabaseObjectCleanUpAfterHugeInsertsTestSqlServerModel> e)
        {
        }
    }
}