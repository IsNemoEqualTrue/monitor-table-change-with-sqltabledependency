using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.ManagedDataAccess.Client;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.OracleClient;

namespace TableDependency.IntegrationTest
{
    public class NoProblemDurignCommandTimeoutForNoMessagesTestOracleModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
        public DateTime Born { get; set; }
        public int Quantity { get; set; }
    }

    [TestClass]
    public class NoProblemDurignCommandTimeoutForNoMessagesTestOracle
    {
        private static string _dbObjectsNaming;
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static readonly string TableName = "AAAA_TIMEOUT".ToUpper();
        private static readonly Dictionary<string, Tuple<NoProblemDurignCommandTimeoutForNoMessagesTestOracleModel, NoProblemDurignCommandTimeoutForNoMessagesTestOracleModel>> CheckValues = new Dictionary<string, Tuple<NoProblemDurignCommandTimeoutForNoMessagesTestOracleModel, NoProblemDurignCommandTimeoutForNoMessagesTestOracleModel>>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            OracleHelper.DropTable(ConnectionString, TableName);

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {TableName} (ID number(10), NAME varchar2(50), DESCRIPTION varchar2(4000))";
                    command.ExecuteNonQuery();
                }
            }
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            OracleHelper.DropTable(ConnectionString, TableName);
        }

        [TestCategory("Oracle")]
        [TestMethod()]
        public void Test()
        {
            var domaininfo = new AppDomainSetup();
            domaininfo.ApplicationBase = Environment.CurrentDirectory;
            var adevidence = AppDomain.CurrentDomain.Evidence;
            var domain = AppDomain.CreateDomain("RunsInAnotherAppDomainNoMessageOrc", adevidence, domaininfo);
            var otherDomainObject = (RunsInAnotherAppDomainNoMessageOrc)domain.CreateInstanceAndUnwrap(typeof(RunsInAnotherAppDomainNoMessageOrc).Assembly.FullName, typeof(RunsInAnotherAppDomainNoMessageOrc).FullName);
            _dbObjectsNaming = otherDomainObject.RunTableDependency(ConnectionString, TableName);
            Thread.Sleep(4 * 60 * 1000);
            var status = otherDomainObject.GetTableDependencyStatus();
            Thread.Sleep(3 * 60 * 1000);
            AppDomain.Unload(domain);

            Assert.IsTrue(status != TableDependencyStatus.StoppedDueToError && status != TableDependencyStatus.StoppedDueToCancellation);
            Assert.IsTrue(OracleHelper.AreAllDbObjectsDisposed(ConnectionString, _dbObjectsNaming));
        }

        public class RunsInAnotherAppDomainNoMessageOrc : MarshalByRefObject
        {
            OracleTableDependency<NoProblemDurignCommandTimeoutForNoMessagesTestOracleModel> _tableDependency = null;

            public TableDependencyStatus GetTableDependencyStatus()
            {
                var status = this._tableDependency.Status;
                this._tableDependency.Stop();
                this._tableDependency.Dispose();
                return status;
            }

            public string RunTableDependency(string connectionString, string tableName)
            {
                this._tableDependency = new OracleTableDependency<NoProblemDurignCommandTimeoutForNoMessagesTestOracleModel>(connectionString, tableName);
                this._tableDependency.OnChanged += TableDependency_Changed;
                this._tableDependency.Start(60, 120);
                return this._tableDependency.DataBaseObjectsNamingConvention;
            }

            private static void TableDependency_Changed(object sender, RecordChangedEventArgs<NoProblemDurignCommandTimeoutForNoMessagesTestOracleModel> e)
            {
            }
        }
    }
}