using System.Collections.Generic;
using System.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Exceptions;
using TableDependency.Mappers;
using TableDependency.OracleClient.IntegrationTest.Model;

namespace TableDependency.OracleClient.IntegrationTest
{
    [TestClass]
    public class PreliminaryTests
    {
        private string ValidConnectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private string ValidTableName = ConfigurationManager.AppSettings.Get("tableName");
        private string InvalidValidConnectionString = "data source=.;initial catalog=NotExistingDB;integrated security=True";
        private string InvalidTableName = "NotExistingTable";

        [TestInitialize]
        public void TestInitialize()
        {
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidConnectionStringException))]
        public void InvalidConnectionStringTest()
        {
            using (new OracleTableDependency<Item>(InvalidValidConnectionString, ValidTableName))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(NotExistingTableException))]
        public void InvalidTableNameTest()
        {
            using (new OracleTableDependency<Item>(this.ValidConnectionString, InvalidTableName))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void EmptyMappertTest()
        {
            var mapper = new ModelToTableMapper<Item>();

            using (new OracleTableDependency<Item>(this.ValidConnectionString, ValidTableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void MappertWithNullTest()
        {
            var mapper = new ModelToTableMapper<Item>();
            mapper.AddMapping(c => c.Description, "Long Description").AddMapping(c => c.Name, null);

            using (new OracleTableDependency<Item>(this.ValidConnectionString, ValidTableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void MappertWithEmptyTest()
        {
            var mapper = new ModelToTableMapper<Item>();
            mapper.AddMapping(c => c.Description, "Long Description").AddMapping(c => c.Name, string.Empty);

            using (new OracleTableDependency<Item>(this.ValidConnectionString, ValidTableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void InvalidMappertTest()
        {
            var mapper = new ModelToTableMapper<Item>();
            mapper.AddMapping(c => c.Description, "Long Description").AddMapping(c => c.Name, "Not Exist");

            using (new OracleTableDependency<Item>(this.ValidConnectionString, ValidTableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void EmptyUpdateOfListTest()
        {
            using (new OracleTableDependency<Item>(this.ValidConnectionString, ValidTableName, updateOfList: new List<string>()))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void UpdateOfListWithNullTest()
        {
            using (new OracleTableDependency<Item>(this.ValidConnectionString, ValidTableName, updateOfList: new List<string>() { "NAME", null }))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void UpdateOfListWithEmptyTest()
        {
            using (new OracleTableDependency<Item>(this.ValidConnectionString, ValidTableName, updateOfList: new List<string>() { "NAME", string.Empty }))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void InvalidUpdateOfListTest()
        {
            using (new OracleTableDependency<Item>(this.ValidConnectionString, ValidTableName, updateOfList: new List<string>() { "Not exists" }))
            {
            }                             
        }
    }
}