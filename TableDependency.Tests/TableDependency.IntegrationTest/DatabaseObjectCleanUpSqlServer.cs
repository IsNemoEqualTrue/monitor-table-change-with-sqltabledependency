using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.IntegrationTest.Helpers.SqlServer;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
#if DEBUG
    [TestClass]
    public class DatabaseObjectCleanUpSqlServer
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServerConnectionString"].ConnectionString;
        private static string TableName = "DatabaseObjectCleanUpSqlServer";
        public static string _dbObjectsNaming;

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
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
            using (var sqlConnection = new SqlConnection(ConnectionString))
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
            var tableDependency = new SqlTableDependency<EventForAllColumnsTestSqlServerModel>(ConnectionString, TableName);
            tableDependency.OnChanged += TableDependency_OnChanged;
            tableDependency.Start();
            _dbObjectsNaming = tableDependency.DataBaseObjectsNamingConvention;

            Thread.Sleep(5000);
            
            tableDependency.StopWithoutDisposing();

            Thread.Sleep(4 * 60 * 1000);
            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, _dbObjectsNaming));
        }

        private void TableDependency_OnChanged(object sender, EventArgs.RecordChangedEventArgs<EventForAllColumnsTestSqlServerModel> e)
        {
        }
    }
#endif
}