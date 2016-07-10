using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.ManagedDataAccess.Client;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.Mappers;
using TableDependency.OracleClient;

namespace TableDependency.IntegrationTest
{
    public class NullMessageContentTestOracleModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
    }

    [TestClass]
    public class NullMessageContentTestOracle
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static readonly string TableName = "AAAA_NULL".ToUpper();
        private static int _id;
        private static string _name;
        private static string _description;

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            OracleHelper.DropTable(ConnectionString, TableName);

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {TableName} (ID NUMBER(10), NAME VARCHAR2(50), \"Long Description\" VARCHAR2(4000))";
                    command.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize()]
        public void TestInitialize()
        {
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            OracleHelper.DropTable(ConnectionString, TableName);
        }

        [TestCategory("Oracle")]
        [TestMethod]
        public void Test1()
        {
            OracleTableDependency<NullMessageContentTestOracleModel> tableDependency = null;
            string naming = null;

            try
            {
                var mapper = new ModelToTableMapper<NullMessageContentTestOracleModel>();
                mapper.AddMapping(c => c.Description, "Long Description");

                tableDependency = new OracleTableDependency<NullMessageContentTestOracleModel>(ConnectionString, TableName, mapper);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;
                Thread.Sleep(1000);

                var t = new Task(ModifyTableContent1);
                t.Start();
                t.Wait(5000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.IsTrue(_id > 0);
            Assert.IsTrue(string.IsNullOrWhiteSpace(_name));
            Assert.IsTrue(string.IsNullOrWhiteSpace(_description));
            Assert.IsTrue(OracleHelper.AreAllDbObjectsDisposed(ConnectionString, naming));
        }

        [TestCategory("Oracle")]
        [TestMethod]
        public void Test2()
        {
            OracleTableDependency<NullMessageContentTestOracleModel> tableDependency = null;
            string naming = null;

            try
            {
                var mapper = new ModelToTableMapper<NullMessageContentTestOracleModel>();
                mapper.AddMapping(c => c.Description, "Long Description");

                tableDependency = new OracleTableDependency<NullMessageContentTestOracleModel>(ConnectionString, TableName, mapper);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;
                Thread.Sleep(1000);

                var t = new Task(ModifyTableContent2);
                t.Start();
                t.Wait(5000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.IsTrue(_id > 0);
            Assert.IsTrue(string.IsNullOrWhiteSpace(_name));
            Assert.IsTrue(string.IsNullOrWhiteSpace(_description));
            Assert.IsTrue(OracleHelper.AreAllDbObjectsDisposed(ConnectionString, naming));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<NullMessageContentTestOracleModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _id = e.Entity.Id;
                    _name = e.Entity.Name;
                    _description = e.Entity.Description;
                    break;
            }
        }

        private static void ModifyTableContent1()
        {
            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"BEGIN INSERT INTO {TableName} (ID) VALUES (123); END;";
                    command.ExecuteNonQuery();
                    Thread.Sleep(2000);
                }
            }
        }

        private static void ModifyTableContent2()
        {
            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"BEGIN INSERT INTO {TableName} (ID, NAME, \"Long Description\") VALUES (123, '', ''); END;";
                    command.ExecuteNonQuery();
                    Thread.Sleep(2000);
                }
            }
        }
    }
}