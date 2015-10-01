using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.SqlClient.IntegrationTest.Helpers;
using TableDependency.SqlClient.IntegrationTest.Model;

namespace TableDependency.SqlClient.IntegrationTest
{
    [TestClass]
    public class Check_DatabaseObjectCleanUp
    {
        private static string _dbObjectsNaming;
        private static string _connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private static string TableName = "Check_Model";

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
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

        [TestInitialize()]
        public void TestInitialize()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestMethod]
        public void DatabaseObjectCleanUpTest()
        {
            var domaininfo = new AppDomainSetup();
            domaininfo.ApplicationBase = Environment.CurrentDirectory;
            var adevidence = AppDomain.CurrentDomain.Evidence;
            var domain = AppDomain.CreateDomain("TableDependencyDomain", adevidence, domaininfo);
            var otherDomainObject = (RunsInAnotherAppDomain)domain.CreateInstanceAndUnwrap(typeof(RunsInAnotherAppDomain).Assembly.FullName, typeof(RunsInAnotherAppDomain).FullName);
            _dbObjectsNaming = otherDomainObject.RunTableDependency(_connectionString, TableName);
            Thread.Sleep(5000);
            AppDomain.Unload(domain);

            Thread.Sleep(3 * 60 * 1000);
            Assert.IsTrue(Helper.AreAllDbObjectDisposed(_connectionString, _dbObjectsNaming));
        }
    }

    public class RunsInAnotherAppDomain : MarshalByRefObject
    {
        public string RunTableDependency(string connectionString, string tableName)
        {
            var mapper = new ModelToTableMapper<Check_Model>();
            mapper.AddMapping(c => c.Name, "First Name").AddMapping(c => c.Surname, "Second Name");

            var tableDependency = new SqlTableDependency<Check_Model>(connectionString, tableName, mapper);
            tableDependency.OnChanged += TableDependency_Changed;
            tableDependency.Start(60, 120);
            return tableDependency.DataBaseObjectsNamingConvention;
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Check_Model> e)
        {
        }
    }
}