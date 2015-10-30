using System;
using System.Configuration;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.DataAccess.Client;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.Mappers;
using TableDependency.OracleClient;

namespace TableDependency.IntegrationTest
{
    public class DatabaseObjectCleanUpTestOracleModel
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public int Qty { get; set; }
    }

    [TestClass]
    public class DatabaseObjectCleanUpTestOracle
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static readonly string TableName = "AAAA_Table".ToUpper();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            OracleHelper.DropTable(ConnectionString, TableName);
        }

        [TestInitialize()]
        public void TestInitialize()
        {
            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {TableName} (ID number(10), NAME varchar2(50), \"Long Description\" varchar2(4000))";
                    command.ExecuteNonQuery();
                }
            }
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            OracleHelper.DropTable(ConnectionString, TableName);
        }

        /// <summary>
        /// THIS TEST MUST BE EXECUTED IN Debug RELEASE !!!
        /// </summary>
        [TestMethod]
        public void DatabaseObjectCleanUpTest()
        {
            var domaininfo = new AppDomainSetup {ApplicationBase = Environment.CurrentDirectory};
            var adevidence = AppDomain.CurrentDomain.Evidence;
            var domain = AppDomain.CreateDomain("AppDomainOracleCleannUpOracle", adevidence, domaininfo);
            var otherDomainObject = (AppDomainOracleCleannUpOracle)domain.CreateInstanceAndUnwrap(typeof(AppDomainOracleCleannUpOracle).Assembly.FullName, typeof(AppDomainOracleCleannUpOracle).FullName);
            var dbObjectsNaming = otherDomainObject.RunTableDependency(ConnectionString, TableName);           
            otherDomainObject.StopTableDependency();

            Thread.Sleep(3 * 60 * 1000);
            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, dbObjectsNaming));
        }

        public class AppDomainOracleCleannUpOracle : MarshalByRefObject
        {
            OracleTableDependency<DatabaseObjectCleanUpTestOracleModel> _tableDependency = null;

            public string RunTableDependency(string connectionString, string tableName)
            {
                var mapper = new ModelToTableMapper<DatabaseObjectCleanUpTestOracleModel>();
                mapper.AddMapping(c => c.Description, "Long Description");

                this._tableDependency = new OracleTableDependency<DatabaseObjectCleanUpTestOracleModel>(connectionString, tableName, mapper);
                this._tableDependency.OnChanged += TableDependency_Changed;
                this._tableDependency.Start(60, 120);
                Thread.Sleep(2000);
                return this._tableDependency.DataBaseObjectsNamingConvention;
            }

            public void StopTableDependency()
            {
#if DEBUG
                _tableDependency.StopMantainingDatabaseObjects();
#endif
            }

            private static void TableDependency_Changed(object sender, RecordChangedEventArgs<DatabaseObjectCleanUpTestOracleModel> e)
            {
            }
        }
    }
}