using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.IntegrationTest.Base;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    [TestClass]
    public class DatabaseObjectCleanUpAfterHugeInsertsTestSqlServer : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = "DatabaseObjectCleanUpAfterHugeInsertsTestSqlServer";
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
        public void DatabaseObjectCleanUpTest()
        {
            var mapper = new ModelToTableMapper<EventForAllColumnsTestSqlServerModel>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second Name");

            var tableDependency = new SqlTableDependency<EventForAllColumnsTestSqlServerModel>(ConnectionStringForTestUser, TableName, mapper);
            tableDependency.OnChanged += TableDependency_OnChanged;
            tableDependency.Start();
            DbObjectsNaming = tableDependency.DataBaseObjectsNamingConvention;

            Thread.Sleep(50);

            var t = new Task(BigModifyTableContent);
            t.Start();
            Thread.Sleep(1000 * 10 * 1);

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

        private void TableDependency_OnChanged(object sender, EventArgs.RecordChangedEventArgs<EventForAllColumnsTestSqlServerModel> e)
        {
        }
    }
}