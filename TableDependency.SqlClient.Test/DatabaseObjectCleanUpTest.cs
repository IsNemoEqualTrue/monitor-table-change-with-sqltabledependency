using System.Data.SqlClient;
using System.Threading;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Base.EventArgs;
using TableDependency.SqlClient.Test.Base;
using TableDependency.SqlClient.Test.Inheritance;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class DatabaseObjectCleanUpTest : SqlTableDependencyBaseTest
    {
        private class DatabaseObjectCleanUpSqlServerModel
        {
            public int Id { get; set; }
            public string Description { get; set; }
        }

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
        public void Test()
        {
            var tableDependency = new SqlTableDependencyTest<DatabaseObjectCleanUpSqlServerModel>(
                ConnectionStringForTestUser, 
                tableName: TableName);

            tableDependency.OnChanged += TableDependency_OnChanged;
            tableDependency.Start();
            var dbObjectsNaming = tableDependency.DataBaseObjectsNamingConvention;

            Thread.Sleep(10000);

            tableDependency.Stop();

            Thread.Sleep(1 * 60 * 1000);
            Assert.IsTrue(base.AreAllDbObjectDisposed(dbObjectsNaming));
            Assert.IsTrue(base.CountConversationEndpoints(dbObjectsNaming) == 0);
        }

        private void TableDependency_OnChanged(object sender, RecordChangedEventArgs<DatabaseObjectCleanUpSqlServerModel> e)
        {
        }
    }
}