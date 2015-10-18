using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.DataAccess.Client;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.IntegrationTest.Models;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    [TestClass]
    public class NoProblemDurignCommandTimeoutForNoMessagesTestOracle
    {
        private static string _dbObjectsNaming;
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static readonly string TableName = "AAAA_Table".ToUpper();
        private static int _counter = 0;
        private static readonly Dictionary<string, Tuple<Item, Item>> CheckValues = new Dictionary<string, Tuple<Item, Item>>();

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

        [TestInitialize()]
        public void TestInitialize()
        {
            var domaininfo = new AppDomainSetup();
            domaininfo.ApplicationBase = Environment.CurrentDirectory;
            var adevidence = AppDomain.CurrentDomain.Evidence;
            var domain = AppDomain.CreateDomain("RunsInAnotherAppDomainNoMessageOrc", adevidence, domaininfo);
            var otherDomainObject = (RunsInAnotherAppDomainNoMessageOrc)domain.CreateInstanceAndUnwrap(typeof(RunsInAnotherAppDomainNoMessageOrc).Assembly.FullName, typeof(RunsInAnotherAppDomainNoMessageOrc).FullName);
            _dbObjectsNaming = otherDomainObject.RunTableDependency(ConnectionString, TableName);
            Thread.Sleep(4 * 60 * 1000);
            var status = otherDomainObject.GetTableDependencyStatus();
            AppDomain.Unload(domain);
            Thread.Sleep(3 * 60 * 1000);

            Assert.IsTrue(status != TableDependencyStatus.StoppedDueToError && status != TableDependencyStatus.StoppedDueToCancellation);
            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, _dbObjectsNaming));
        }

        public class RunsInAnotherAppDomainNoMessageOrc : MarshalByRefObject
        {
            SqlTableDependency<Check_Model> _tableDependency = null;

            public TableDependencyStatus GetTableDependencyStatus()
            {
                return this._tableDependency.Status;
            }

            public string RunTableDependency(string connectionString, string tableName)
            {
                this._tableDependency = new SqlTableDependency<Check_Model>(connectionString, tableName);
                this._tableDependency.OnChanged += TableDependency_Changed;
                this._tableDependency.Start(60, 120);
                return this._tableDependency.DataBaseObjectsNamingConvention;
            }

            private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Check_Model> e)
            {
            }
        }
    }
}