using System.ComponentModel.DataAnnotations.Schema;
using System.Configuration;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.ManagedDataAccess.Client;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.Mappers;
using TableDependency.OracleClient;

namespace TableDependency.IntegrationTest
{
    public class TaskCancellationTestOracleModel
    {
        public long Id { get; set; }
        public string Name { get; set; }
        [Column(ColumnName)]
        public string FamilyName { get; set; }
        private const string ColumnName = "SURNAME";
        public static string GetColumnName => ColumnName;
    }

    [TestClass]
    public class TaskCancellationTestOracle
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

        [TestMethod]
        public void TaskCancellationTest()
        {
            string naming = null;
            OracleTableDependency<TaskCancellationTestOracleModel> tableDependency = null;

            try
            {
                var mapper = new ModelToTableMapper<TaskCancellationTestOracleModel>();
                mapper.AddMapping(c => c.Name, "Long Description");

                tableDependency = new OracleTableDependency<TaskCancellationTestOracleModel>(ConnectionString, TableName, mapper);
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

            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }
    }
}