using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.IntegrationTest.Helpers.SqlServer;
using TableDependency.Mappers;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    [TestClass]
    public class DatabaseObjectCleanUpAfterHugeInsertsTestSqlServer
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServerConnectionString"].ConnectionString;
        private static string TableName = "DisposeMe";
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

        [AssemblyCleanup()]
        public static void AssemblyCleanup()
        {
            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, _dbObjectsNaming));
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void DatabaseObjectCleanUpTest2()
        {
            var mapper = new ModelToTableMapper<EventForAllColumnsTestSqlServerModel>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second Name");

            var tableDependency = new SqlTableDependency<EventForAllColumnsTestSqlServerModel>(ConnectionString, TableName, mapper);
            tableDependency.OnChanged += TableDependency_OnChanged;
            tableDependency.Start();
            _dbObjectsNaming = tableDependency.DataBaseObjectsNamingConvention;

            Thread.Sleep(500);

            var t = new Task(ModifyTableContent);
            t.Start();
            t.Wait(10000);

            Thread.Sleep(1000 * 60 * 1);
            tableDependency.Stop();
        }

        private static void ModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
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

        private void TableDependency_OnChanged(object sender, EventArgs.RecordChangedEventArgs<EventForAllColumnsTestSqlServerModel> e)
        {
        }
    }
}