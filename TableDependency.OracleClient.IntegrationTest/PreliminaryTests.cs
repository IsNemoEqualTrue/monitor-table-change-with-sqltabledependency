using System.Collections.Generic;
using System.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.DataAccess.Client;
using TableDependency.Exceptions;
using TableDependency.Mappers;
using TableDependency.OracleClient.IntegrationTest.Helpers;
using TableDependency.OracleClient.IntegrationTest.Model;

namespace TableDependency.OracleClient.IntegrationTest
{
    [TestClass]
    public class PreliminaryTests
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private static readonly string TableName = "AAAA_Table".ToUpper();
        private static string InvalidValidConnectionString = "data source=.;initial catalog=NotExistingDB;integrated security=True";
        private static string InvalidTableName = "NotExistingTable";

        [ClassInitialize()]

        public static void ClassInitialize(TestContext testContext)
        {
            Helper.DropTable(ConnectionString, TableName);
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
            Helper.DropTable(ConnectionString, TableName);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidConnectionStringException))]
        public void InvalidConnectionStringTest()
        {
            using (new OracleTableDependency<Item>(InvalidValidConnectionString, TableName))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(NotExistingTableException))]
        public void InvalidTableNameTest()
        {
            using (new OracleTableDependency<Item>(ConnectionString, InvalidTableName))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void EmptyMappertTest()
        {
            var mapper = new ModelToTableMapper<Item>();

            using (new OracleTableDependency<Item>(ConnectionString, TableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void MappertWithNullTest()
        {
            var mapper = new ModelToTableMapper<Item>();
            mapper.AddMapping(c => c.Description, "Long Description").AddMapping(c => c.Name, null);

            using (new OracleTableDependency<Item>(ConnectionString, TableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void MappertWithEmptyTest()
        {
            var mapper = new ModelToTableMapper<Item>();
            mapper.AddMapping(c => c.Description, "Long Description").AddMapping(c => c.Name, string.Empty);

            using (new OracleTableDependency<Item>(ConnectionString, TableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void InvalidMappertTest()
        {
            var mapper = new ModelToTableMapper<Item>();
            mapper.AddMapping(c => c.Description, "Long Description").AddMapping(c => c.Name, "Not Exist");

            using (new OracleTableDependency<Item>(ConnectionString, TableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void EmptyUpdateOfListTest()
        {
            using (new OracleTableDependency<Item>(ConnectionString, TableName, updateOf: new List<string>()))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void UpdateOfListWithNullTest()
        {
            using (new OracleTableDependency<Item>(ConnectionString, TableName, updateOf: new List<string>() { "NAME", null }))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void UpdateOfListWithEmptyTest()
        {
            using (new OracleTableDependency<Item>(ConnectionString, TableName, updateOf: new List<string>() { "NAME", string.Empty }))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void InvalidUpdateOfListTest()
        {
            using (new OracleTableDependency<Item>(ConnectionString, TableName, updateOf: new List<string>() { "Not exists" }))
            {
            }                             
        }
    }
}