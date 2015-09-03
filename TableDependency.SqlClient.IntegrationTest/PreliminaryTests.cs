using System.Collections.Generic;
using System.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Exceptions;
using TableDependency.Mappers;
using TableDependency.SqlClient.IntegrationTest.Model;

namespace TableDependency.SqlClient.IntegrationTest
{
    [TestClass]
    public class PreliminaryTests
    {
        private string ValidConnectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private const string ValidTableName = "Customer";
        private const string InvalidValidConnectionString = "data source=.;initial catalog=NotExistingDB;integrated security=True";
        private const string InvalidTableName = "NotExistingTable";

        [TestInitialize]
        public void TestInitialize()
        {
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidConnectionStringException))]
        public void InvalidConnectionStringTest()
        {
            using (new SqlTableDependency<Customer>(InvalidValidConnectionString, ValidTableName))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(NotExistingTableException))]
        public void InvalidTableNameTest()
        {
            using (new SqlTableDependency<Customer>(ValidConnectionString, InvalidTableName))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void EmptyMappertTest()
        {
            var mapper = new ModelToTableMapper<Customer>();

            using (new SqlTableDependency<Customer>(ValidConnectionString, ValidTableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void MappertWithNullTest()
        {
            var mapper = new ModelToTableMapper<Customer>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, null);

            using (new SqlTableDependency<Customer>(ValidConnectionString, ValidTableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void MappertWithEmptyTest()
        {
            var mapper = new ModelToTableMapper<Customer>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, string.Empty);

            using (new SqlTableDependency<Customer>(ValidConnectionString, ValidTableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void InvalidMappertTest()
        {
            var mapper = new ModelToTableMapper<Customer>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Not Exist");

            using (new SqlTableDependency<Customer>(ValidConnectionString, ValidTableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void EmptyUpdateOfListTest()
        {
            using (new SqlTableDependency<Customer>(ValidConnectionString, ValidTableName, columnsToMonitorDuringUpdate: new List<string>()))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void UpdateOfListWithNullTest()
        {
            using (new SqlTableDependency<Customer>(ValidConnectionString, ValidTableName, columnsToMonitorDuringUpdate: new List<string>() { "Second Name", null }))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void UpdateOfListWithEmptyTest()
        {
            using (new SqlTableDependency<Customer>(ValidConnectionString, ValidTableName, columnsToMonitorDuringUpdate: new List<string>() { "Second Name", string.Empty }))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void InvalidUpdateOfListTest()
        {
            using (new SqlTableDependency<Customer>(ValidConnectionString, ValidTableName, columnsToMonitorDuringUpdate: new List<string>() { "Not exists" }))
            {
            }                             
        }
    }
}