using System.Configuration;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Mappers;
using TableDependency.OracleClient.IntegrationTest.Helpers;
using TableDependency.OracleClient.IntegrationTest.Model;
using Oracle.DataAccess.Client;

namespace TableDependency.OracleClient.IntegrationTest
{
    [TestClass]
    public class TaskCancellation
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private static readonly string TableName = "AAAA_Table".ToUpper();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            Helper.DropTable(ConnectionString, TableName);
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
            Helper.DropTable(ConnectionString, TableName);
        }

        [TestMethod]
        public void TaskCancellationTest()
        {
            string naming = null;
            OracleTableDependency<Item> tableDependency = null;

            try
            {
                var mapper = new ModelToTableMapper<Item>();
                mapper.AddMapping(c => c.Description, "Long Description");

                tableDependency = new OracleTableDependency<Item>(ConnectionString, TableName, mapper);
                tableDependency.OnChanged += (sender, e) => { };
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                tableDependency.Stop();

                Thread.Sleep(5000);
            }
            catch
            {
                tableDependency?.Dispose();
            }

            Assert.IsTrue(Helper.AreAllDbObjectDisposed(ConnectionString, naming));
        }
    }
}