using System;
using System.Data.SqlClient;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.IntegrationTest.Base;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
#if DEBUG
    public class DatabaseObjectAutoCleanUpAfterHugeInsertsTestSqlServerModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
        public DateTime Born { get; set; }
        public int Quantity { get; set; }
    }

    [TestClass]
    public class DatabaseObjectAutoCleanUpAfterHugeInsertsTestSqlServer : SqlTableDependencyBaseTest
    {
        private static string TableName = "DatabaseObjectAutoCleanUpAfterHugeInsertsTestSqlServerModel";

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

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [int], [First Name] [nvarchar](50), [Second Name] [nvarchar](50))";
                    sqlCommand.ExecuteNonQuery();
                }
                sqlConnection.Close();
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
                sqlConnection.Close();
            }
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void DatabaseObjectCleanUpTest()
        {
            var mapper = new ModelToTableMapper<DatabaseObjectAutoCleanUpAfterHugeInsertsTestSqlServerModel>();
            mapper.AddMapping(c => c.Name, "First Name").AddMapping(c => c.Surname, "Second Name");

            var tableDependency = new SqlTableDependency<DatabaseObjectAutoCleanUpAfterHugeInsertsTestSqlServerModel>(ConnectionStringForTestUser, TableName, mapper);
            tableDependency.OnChanged += TableDependency_OnChanged;
            tableDependency.Start();
            var dbObjectsNaming = tableDependency.DataBaseObjectsNamingConvention;

            Thread.Sleep(5000);

            tableDependency.StopWithoutDisposing();

            Thread.Sleep(1000);

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    for (var i = 0; i < 10000; i++)
                    {
                        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{i}', '{i}')";
                        sqlCommand.ExecuteNonQuery();
                    }
                }
                sqlConnection.Close();
            }

            Thread.Sleep(1000 * 60 * 3);

            Assert.IsTrue(base.AreAllDbObjectDisposed(dbObjectsNaming));
            Assert.IsTrue(base.CountConversationEndpoints(dbObjectsNaming) == 0);
        }

        private static void TableDependency_OnChanged(object sender, EventArgs.RecordChangedEventArgs<DatabaseObjectAutoCleanUpAfterHugeInsertsTestSqlServerModel> e)
        {
        }
    }
#endif
}