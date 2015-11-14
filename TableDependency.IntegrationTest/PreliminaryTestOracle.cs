using System.Collections.Generic;
using System.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.ManagedDataAccess.Client;
using TableDependency.Exceptions;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.Mappers;
using TableDependency.OracleClient;

namespace TableDependency.IntegrationTest
{
    public class PreliminaryTestOracleModel
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public string Infos { get; set; }
    }

    [TestClass]
    public class PreliminaryTestOracle
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static readonly string TableName = "AAAA_Table".ToUpper();
        private static string InvalidValidConnectionString = "data source=.;initial catalog=NotExistingDB;integrated security=True";
        private static string InvalidTableName = "NotExistingTable";

        [ClassInitialize()]

        public static void ClassInitialize(TestContext testContext)
        {
            OracleHelper.DropTable(ConnectionString, TableName);
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
        [ExpectedException(typeof(InvalidConnectionStringException))]
        public void InvalidConnectionStringTest()
        {
            using (new OracleTableDependency<PreliminaryTestOracleModel>(InvalidValidConnectionString, TableName))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(NotExistingTableException))]
        public void NotExistingTableNameFromModelTest()
        {
            using (new OracleTableDependency<PreliminaryTestOracleModel>(ConnectionString))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void EmptyUpdateOfModelListTest()
        {
            using (new OracleTableDependency<PreliminaryTestOracleModel>(ConnectionString, TableName, updateOf: new UpdateOfModel<PreliminaryTestOracleModel>()))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(NotExistingTableException))]
        public void InvalidTableNameTest()
        {
            using (new OracleTableDependency<PreliminaryTestOracleModel>(ConnectionString, InvalidTableName))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void EmptyMappertTest()
        {
            var mapper = new ModelToTableMapper<PreliminaryTestOracleModel>();

            using (new OracleTableDependency<PreliminaryTestOracleModel>(ConnectionString, TableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void MappertWithNullTest()
        {
            var mapper = new ModelToTableMapper<PreliminaryTestOracleModel>();
            mapper.AddMapping(c => c.Name, "Long Description").AddMapping(c => c.Infos, null);

            using (new OracleTableDependency<PreliminaryTestOracleModel>(ConnectionString, TableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void MappertWithEmptyTest()
        {
            var mapper = new ModelToTableMapper<PreliminaryTestOracleModel>();
            mapper.AddMapping(c => c.Name, "Long Description").AddMapping(c => c.Infos, string.Empty);

            using (new OracleTableDependency<PreliminaryTestOracleModel>(ConnectionString, TableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void InvalidMappertTest()
        {
            var mapper = new ModelToTableMapper<PreliminaryTestOracleModel>();
            mapper.AddMapping(c => c.Infos, "Long Description").AddMapping(c => c.Name, "Not Exist");

            using (new OracleTableDependency<PreliminaryTestOracleModel>(ConnectionString, TableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void EmptyUpdateOfListTest()
        {
            using (new OracleTableDependency<PreliminaryTestOracleModel>(ConnectionString, TableName, updateOf: new List<string>()))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void UpdateOfListWithNullTest()
        {
            using (new OracleTableDependency<PreliminaryTestOracleModel>(ConnectionString, TableName, updateOf: new List<string>() { "NAME", null }))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void UpdateOfListWithEmptyTest()
        {
            using (new OracleTableDependency<PreliminaryTestOracleModel>(ConnectionString, TableName, updateOf: new List<string>() { "NAME", string.Empty }))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void InvalidUpdateOfListTest()
        {
            using (new OracleTableDependency<PreliminaryTestOracleModel>(ConnectionString, TableName, updateOf: new List<string>() { "Not exists" }))
            {
            }                             
        }
    }
}