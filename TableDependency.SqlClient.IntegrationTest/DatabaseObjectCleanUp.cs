using System;
using System.Configuration;
using System.Security.Policy;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.SqlClient.IntegrationTest.Helpers;
using TableDependency.SqlClient.IntegrationTest.Model;

namespace TableDependency.SqlClient.IntegrationTest
{
    [TestClass]
    public class DatabaseObjectCleanUp
    {
        private static string _dbObjectsNaming;
        private static string _connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private static string _tableName = "Customer";

        [TestMethod]
        public void DatabaseObjectCleanUpTest()
        {
            var domaininfo = new AppDomainSetup();
            domaininfo.ApplicationBase = Environment.CurrentDirectory;
            var adevidence = AppDomain.CurrentDomain.Evidence;
            var domain = AppDomain.CreateDomain("TableDependencyDomain", adevidence, domaininfo);
            var otherDomainObject = (RunsInAnotherAppDomain)domain.CreateInstanceAndUnwrap(typeof(RunsInAnotherAppDomain).Assembly.FullName, typeof(RunsInAnotherAppDomain).FullName);
            _dbObjectsNaming = otherDomainObject.RunTableDependency(_connectionString, _tableName);
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
            var mapper = new ModelToTableMapper<Customer>();
            mapper.AddMapping(c => c.Name, "First Name").AddMapping(c => c.Surname, "Second Name");

            var tableDependency = new SqlTableDependency<Customer>(connectionString, tableName, mapper);
            tableDependency.OnChanged += TableDependency_Changed;
            tableDependency.Start(60, 120);
            return tableDependency.DataBaseObjectsNamingConvention;
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Customer> e)
        {
        }
    }
}