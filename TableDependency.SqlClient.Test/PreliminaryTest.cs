using System;
using System.Data.SqlClient;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Base;
using TableDependency.SqlClient.Base.Exceptions;
using TableDependency.SqlClient.Exceptions;

namespace TableDependency.SqlClient.Test
{
    public class PreliminaryTestSqlServerModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
        public DateTime Born { get; set; }
        public int Quantity { get; set; }
    }

    [TestClass]
    public class PreliminaryTest : Base.SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(PreliminaryTestSqlServerModel).Name;
        private const string InvalidValidConnectionString = "data source=.;initial catalog=NotExistingDB;integrated security=True";
        private const string InvalidTableName = "NotExistingTable";

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
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

        [TestInitialize]
        public void TestInitialize()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        [ExpectedException(typeof(ImpossibleOpenSqlConnectionException))]
        public void InvalidConnectionStringTest()
        {
            using (new SqlTableDependency<PreliminaryTestSqlServerModel>(InvalidValidConnectionString, TableName))
            {
            }
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        [ExpectedException(typeof(NotExistingTableException))]
        public void InvalidTableNameTest()
        {
            using (new SqlTableDependency<PreliminaryTestSqlServerModel>(ConnectionStringForTestUser, InvalidTableName))
            {
            }
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void MappertWithNullTest()
        {
            var mapper = new ModelToTableMapper<PreliminaryTestSqlServerModel>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, null);

            using (new SqlTableDependency<PreliminaryTestSqlServerModel>(ConnectionStringForTestUser, tableName: TableName, mapper: mapper))
            {
            }
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void MappertWithEmptyTest()
        {
            var mapper = new ModelToTableMapper<PreliminaryTestSqlServerModel>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, string.Empty);

            using (new SqlTableDependency<PreliminaryTestSqlServerModel>(ConnectionStringForTestUser, tableName: TableName, mapper: mapper))
            {
            }
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void InvalidMappertTest()
        {
            var mapper = new ModelToTableMapper<PreliminaryTestSqlServerModel>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Not Exist");

            using (new SqlTableDependency<PreliminaryTestSqlServerModel>(ConnectionStringForTestUser, tableName: TableName, mapper: mapper))
            {
            }
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        [ExpectedException(typeof(UpdateOfException))]
        public void EmptyUpdateOfModelListTest()
        {
            using (new SqlTableDependency<PreliminaryTestSqlServerModel>(ConnectionStringForTestUser, TableName, updateOf: new UpdateOfModel<PreliminaryTestSqlServerModel>()))
            {
            }
        }
    }
}