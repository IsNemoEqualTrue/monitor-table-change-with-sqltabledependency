using System.Data.SqlClient;
using System.Threading;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.IntegrationTest.Base;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
#if DEBUG
    public class DatabaseObjectCleanUpSqlServerModel
    {
        public int Id { get; set; }
        public string Description { get; set; }
    }

    [TestClass]
    public class DatabaseObjectCleanUpSqlServer : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(DatabaseObjectCleanUpSqlServerModel).Name;

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
        public void DatabaseObjectCleanUpTest()
        {
            var tableDependency = new SqlTableDependency<DatabaseObjectCleanUpSqlServerModel>(ConnectionStringForTestUser, tableName: TableName);
            tableDependency.OnChanged += TableDependency_OnChanged;
            tableDependency.Start();
            var dbObjectsNaming = tableDependency.DataBaseObjectsNamingConvention;

            Thread.Sleep(10000);
            
            tableDependency.StopWithoutDisposing();

            Thread.Sleep(4 * 60 * 1000);
            Assert.IsTrue(base.AreAllDbObjectDisposed(dbObjectsNaming));
            Assert.IsTrue(base.CountConversationEndpoints(dbObjectsNaming) == 0);
        }

        private void TableDependency_OnChanged(object sender, EventArgs.RecordChangedEventArgs<DatabaseObjectCleanUpSqlServerModel> e)
        {
        }
    }
#endif
}