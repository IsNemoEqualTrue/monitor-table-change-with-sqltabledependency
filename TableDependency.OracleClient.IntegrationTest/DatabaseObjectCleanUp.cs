using System;
using System.Configuration;
using System.Security.Policy;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.OracleClient.IntegrationTest.Helpers;
using TableDependency.OracleClient.IntegrationTest.Model;

namespace TableDependency.OracleClient.IntegrationTest
{
    [TestClass]
    public class DatabaseObjectCleanUp
    {
        [TestMethod]
        public void DatabaseObjectCleanUpTest()
        {
            AppDomainSetup domaininfo = new AppDomainSetup();
            domaininfo.ApplicationBase = Environment.CurrentDirectory;
            Evidence adevidence = AppDomain.CurrentDomain.Evidence;
            AppDomain domain = AppDomain.CreateDomain("TableDependencyDomain", adevidence, domaininfo);
            RunsInAnotherAppDomain otherDomainObject = (RunsInAnotherAppDomain)domain.CreateInstanceAndUnwrap(typeof(RunsInAnotherAppDomain).Assembly.FullName, typeof(RunsInAnotherAppDomain).FullName);
            var dbObjectsNaming = otherDomainObject.RunTableDependency(ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString, ConfigurationManager.AppSettings.Get("tableName"));           
            otherDomainObject.StopTableDependency();

            Thread.Sleep(3 * 60 * 1000);
            Assert.IsTrue(Helper.AreAllDbObjectDisposed(ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString, dbObjectsNaming));
        }

        public class RunsInAnotherAppDomain : MarshalByRefObject
        {
            OracleTableDependency<Item> _tableDependency = null;

            public string RunTableDependency(string connectionString, string tableName)
            {
                var mapper = new ModelToTableMapper<Item>();
                mapper.AddMapping(c => c.Description, "Long Description");

                _tableDependency = new OracleTableDependency<Item>(connectionString, tableName, mapper);
                _tableDependency.OnChanged += TableDependency_Changed;
                _tableDependency.Start(60, 120);
                Thread.Sleep(2000);
                return _tableDependency.DataBaseObjectsNamingConvention;
            }

            public void StopTableDependency()
            {
                _tableDependency.StopMantainingDatabaseObjects();
            }

            private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Item> e)
            {
            }
        }
    }
}