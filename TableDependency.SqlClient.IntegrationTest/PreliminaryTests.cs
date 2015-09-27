using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
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
        private const string ValidTableName = "Issue0000";
        private const string InvalidValidConnectionString = "data source=.;initial catalog=NotExistingDB;integrated security=True";
        private const string InvalidTableName = "NotExistingTable";




        [TestMethod]
        [ExpectedException(typeof(InvalidConnectionStringException))]
        public void InvalidConnectionStringTest()
        {
            using (new SqlTableDependency<Issue_0000_Model>(InvalidValidConnectionString, ValidTableName))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(NotExistingTableException))]
        public void InvalidTableNameTest()
        {
            using (new SqlTableDependency<Issue_0000_Model>(ValidConnectionString, InvalidTableName))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void EmptyMappertTest()
        {
            var mapper = new ModelToTableMapper<Issue_0000_Model>();

            using (new SqlTableDependency<Issue_0000_Model>(ValidConnectionString, ValidTableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void MappertWithNullTest()
        {
            var mapper = new ModelToTableMapper<Issue_0000_Model>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, null);

            using (new SqlTableDependency<Issue_0000_Model>(ValidConnectionString, ValidTableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void MappertWithEmptyTest()
        {
            var mapper = new ModelToTableMapper<Issue_0000_Model>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, string.Empty);

            using (new SqlTableDependency<Issue_0000_Model>(ValidConnectionString, ValidTableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void InvalidMappertTest()
        {
            var mapper = new ModelToTableMapper<Issue_0000_Model>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Not Exist");

            using (new SqlTableDependency<Issue_0000_Model>(ValidConnectionString, ValidTableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void EmptyUpdateOfListTest()
        {
            using (new SqlTableDependency<Issue_0000_Model>(ValidConnectionString, ValidTableName, updateOf: new List<string>()))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void UpdateOfListWithNullTest()
        {
            using (new SqlTableDependency<Issue_0000_Model>(ValidConnectionString, ValidTableName, updateOf: new List<string>() { "Second Name", null }))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void UpdateOfListWithEmptyTest()
        {
            using (new SqlTableDependency<Issue_0000_Model>(ValidConnectionString, ValidTableName, updateOf: new List<string>() { "Second Name", string.Empty }))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void InvalidUpdateOfListTest()
        {
            using (new SqlTableDependency<Issue_0000_Model>(ValidConnectionString, ValidTableName, updateOf: new List<string>() { "Not exists" }))
            {
            }                             
        }
    }
}