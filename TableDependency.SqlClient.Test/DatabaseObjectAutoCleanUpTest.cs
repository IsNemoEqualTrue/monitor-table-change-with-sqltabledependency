using System;
using System.Data.SqlClient;
using System.Threading;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Base;

namespace TableDependency.SqlClient.Test
{
    public class DatabaseObjectCleanUpTestSqlServerModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
        public DateTime Born { get; set; }
        public int Quantity { get; set; }
    }

    [TestClass]
    public class DatabaseObjectAutoCleanUpTest : Base.SqlTableDependencyBaseTest
    {
        private static string _dbObjectsNaming;
        private const string TableName = "DatabaseObjectAutoCleanUpTestTable";

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

                    sqlCommand.CommandText =
                        $"CREATE TABLE [{TableName}]( " +
                        "[Id][int] IDENTITY(1, 1) NOT NULL, " +
                        "[First Name] [nvarchar](50) NOT NULL, " +
                        "[Second Name] [nvarchar](50) NOT NULL, " +
                        "[Born] [datetime] NULL)";
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
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
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
            var domaininfo = new AppDomainSetup { ApplicationBase = Environment.CurrentDirectory };
            var adevidence = AppDomain.CurrentDomain.Evidence;
            var domain = AppDomain.CreateDomain("RunsInAnotherAppDomain_Check_DatabaseObjectCleanUp", adevidence, domaininfo);
            var otherDomainObject = (RunsInAnotherAppDomainCheckDatabaseObjectCleanUp)domain.CreateInstanceAndUnwrap(typeof(RunsInAnotherAppDomainCheckDatabaseObjectCleanUp).Assembly.FullName, typeof(RunsInAnotherAppDomainCheckDatabaseObjectCleanUp).FullName);
            _dbObjectsNaming = otherDomainObject.RunTableDependency(ConnectionStringForTestUser, tableName: TableName);
            Thread.Sleep(5000);
            AppDomain.Unload(domain);

            SmallModifyTableContent();

            Thread.Sleep(3 * 60 * 1000);
            Assert.IsTrue(base.AreAllDbObjectDisposed(_dbObjectsNaming));
            Assert.IsTrue(base.CountConversationEndpoints(_dbObjectsNaming) == 0);
        }

        private static void SmallModifyTableContent()
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
    }

    public class RunsInAnotherAppDomainCheckDatabaseObjectCleanUp : MarshalByRefObject
    {
        public string RunTableDependency(string connectionString, string tableName)
        {
            var mapper = new ModelToTableMapper<DatabaseObjectCleanUpTestSqlServerModel>();
            mapper.AddMapping(c => c.Name, "First Name").AddMapping(c => c.Surname, "Second Name");

            var tableDependency = new SqlTableDependency<DatabaseObjectCleanUpTestSqlServerModel>(connectionString, tableName: tableName, mapper: mapper);
            tableDependency.OnChanged += (sender, e) => { };
            tableDependency.Start(60, 120);
            return tableDependency.DataBaseObjectsNamingConvention;
        }
    }
}