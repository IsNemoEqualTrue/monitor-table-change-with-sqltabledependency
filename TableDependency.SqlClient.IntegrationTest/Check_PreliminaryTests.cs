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
    public class Check_PreliminaryTests
    {
        private static string _connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private const string TableName = "Check_Model";
        private const string InvalidValidConnectionString = "data source=.;initial catalog=NotExistingDB;integrated security=True";
        private const string InvalidTableName = "NotExistingTable";

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText =
                        $"CREATE TABLE [{TableName}]( " +
                        "[Id][int] IDENTITY(1, 1) NOT NULL, " +
                        "[First Name] [nvarchar](50) NOT NULL, " +
                        "[Second Name] [nvarchar](50) NOT NULL, " +
                        "[Born] [datetime] NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize()]
        public void TestInitialize()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidConnectionStringException))]
        public void InvalidConnectionStringTest()
        {
            using (new SqlTableDependency<Check_Model>(InvalidValidConnectionString, TableName))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(NotExistingTableException))]
        public void InvalidTableNameTest()
        {
            using (new SqlTableDependency<Check_Model>(_connectionString, InvalidTableName))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void EmptyMappertTest()
        {
            var mapper = new ModelToTableMapper<Check_Model>();

            using (new SqlTableDependency<Check_Model>(_connectionString, TableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void MappertWithNullTest()
        {
            var mapper = new ModelToTableMapper<Check_Model>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, null);

            using (new SqlTableDependency<Check_Model>(_connectionString, TableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void MappertWithEmptyTest()
        {
            var mapper = new ModelToTableMapper<Check_Model>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, string.Empty);

            using (new SqlTableDependency<Check_Model>(_connectionString, TableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void InvalidMappertTest()
        {
            var mapper = new ModelToTableMapper<Check_Model>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Not Exist");

            using (new SqlTableDependency<Check_Model>(_connectionString, TableName, mapper))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void EmptyUpdateOfListTest()
        {
            using (new SqlTableDependency<Check_Model>(_connectionString, TableName, updateOf: new List<string>()))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void UpdateOfListWithNullTest()
        {
            using (new SqlTableDependency<Check_Model>(_connectionString, TableName, updateOf: new List<string>() { "Second Name", null }))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void UpdateOfListWithEmptyTest()
        {
            using (new SqlTableDependency<Check_Model>(_connectionString, TableName, updateOf: new List<string>() { "Second Name", string.Empty }))
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void InvalidUpdateOfListTest()
        {
            using (new SqlTableDependency<Check_Model>(_connectionString, TableName, updateOf: new List<string>() { "Not exists" }))
            {
            }                             
        }
    }
}