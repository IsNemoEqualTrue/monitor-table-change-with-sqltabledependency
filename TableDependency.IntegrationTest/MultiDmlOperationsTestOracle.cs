using System.Collections.Generic;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.DataAccess.Client;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.IntegrationTest.Models;
using TableDependency.Mappers;
using TableDependency.OracleClient;

namespace TableDependency.IntegrationTest
{
    [TestClass]
    public class MultiDmlOperationsTestOracle
    {
        private static int _counter = 0;
        private static string _deletedValue;
        
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

        [TestMethod]
        public void EventForSpecificColumnsTest()
        {
            OracleTableDependency<Item> tableDependency = null;

            try
            {
                var mapper = new ModelToTableMapper<Item>();
                mapper.AddMapping(c => c.Description, "Long Description");

                tableDependency = new OracleTableDependency<Item>(ConnectionString, TableName, mapper);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 9);
            Assert.AreEqual(_deletedValue, "XXXXXX");
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Item> e)
        {
            _counter++;

            if (e.ChangeType ==  ChangeType.Delete)
            {
                _deletedValue = e.Entity.Description;
            }
        }

        private static void ModifyTableContent()
        {
            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $"BEGIN " +
                        $"INSERT INTO {TableName} (ID, NAME, \"Long Description\") VALUES (1, 'Long', 'Description'); " +
                        $"INSERT INTO {TableName} (ID, NAME, \"Long Description\") VALUES (2, 'Long', 'Description'); " +
                        $"INSERT INTO {TableName} (ID, NAME, \"Long Description\") VALUES (3, 'Long', 'Description'); " +
                        $"END;";
                    command.ExecuteNonQuery();
                    Thread.Sleep(2000);

                    command.CommandText =
                        $"BEGIN " +
                        $"UPDATE {TableName} SET \"Long Description\" = 'XXXXXX'; " +
                        $"END;";
                    command.ExecuteNonQuery();
                    Thread.Sleep(2000);

                    command.CommandText =
                        $"BEGIN " +
                        $"DELETE FROM {TableName};" +
                        $"END;";
                    command.ExecuteNonQuery();
                    Thread.Sleep(2000);
                }
            }
        }
    }
}