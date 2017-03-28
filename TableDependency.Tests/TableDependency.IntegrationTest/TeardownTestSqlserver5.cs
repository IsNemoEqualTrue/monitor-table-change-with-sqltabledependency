using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.SqlServer;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    public class TeardownTestSqlserver5Model
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
    }

    [TestClass]
    public class TeardownTestSqlserver5
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServer2008 Test_User"].ConnectionString;
        private static readonly string TableName = "TeardownTestSqlserver5Model";

        private static int _counter = 0;
        private static List<TeardownTestSqlserver5Model> _insertedValues = new List<TeardownTestSqlserver5Model>();
        private static List<TeardownTestSqlserver5Model> _notifiedValues = new List<TeardownTestSqlserver5Model>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [int] NOT NULL, [Name] [NVARCHAR](50) NULL, [Long Description] [NVARCHAR](MAX) NULL)";
                    sqlCommand.ExecuteNonQuery();
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
            using (var sqlConnection = new SqlConnection(ConnectionString))
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
        public void TeardownTests()
        {
            SqlTableDependency<TeardownTestSqlserver5Model> tableDependency = null;
            string dataBaseObjectsNamingConvention = null;

            var mapper = new ModelToTableMapper<TeardownTestSqlserver5Model>();
            mapper.AddMapping(c => c.Description, "Long Description");

            ////////////////////////////////////////////////////////
            // First Round
            ////////////////////////////////////////////////////////
            try
            {
                tableDependency = new SqlTableDependency<TeardownTestSqlserver5Model>(ConnectionString, mapper: mapper, teardown: false);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                dataBaseObjectsNamingConvention = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Thread.Sleep(5000);


            ////////////////////////////////////////////////////////
            // Inserd data while sql table dependency is not running
            ////////////////////////////////////////////////////////

            ModifyTableContent();

            ////////////////////////////////////////////////////////
            // Second Round
            ////////////////////////////////////////////////////////

            tableDependency = new SqlTableDependency<TeardownTestSqlserver5Model>(ConnectionString, mapper: mapper, teardown: true, namingForObjectsAlreadyExisting: dataBaseObjectsNamingConvention);
            tableDependency.OnChanged += TableDependency_Changed;
            tableDependency.Start();

            Thread.Sleep(5000);

            tableDependency.Stop();

            Thread.Sleep(3 * 60 * 1000);
            

            Assert.AreEqual(_counter, 1000);
            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, dataBaseObjectsNamingConvention));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<TeardownTestSqlserver5Model> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _notifiedValues.Add(new TeardownTestSqlserver5Model() { Id = e.Entity.Id, Name = e.Entity.Name, Description = e.Entity.Description });
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    for (var i = 0; i < 1000; i++)
                    {
                        _insertedValues.Add(new TeardownTestSqlserver5Model { Id = _counter, Name = "Christian", Description = "Del Bianco" });
                        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name], [Long Description]) VALUES ({_counter}, '{i}', '{i}')";
                        sqlCommand.ExecuteNonQuery();
                        Thread.Sleep(500);

                        _counter++;
                    }
                }
            }
        }
    }
}