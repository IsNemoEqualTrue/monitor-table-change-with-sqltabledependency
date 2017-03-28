using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.SqlServer;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    public class TeardownTestSqlserver3Model
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
    }

    [TestClass]
    public class TeardownTestSqlserver3
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServer2008 Test_User"].ConnectionString;
        private static readonly string TableName = "TeardownTestSqlserver3Model";
        private static int _counter = 1;
        private static List<TeardownTestSqlserver3Model> _insertedValues = new List<TeardownTestSqlserver3Model>();
        private static List<TeardownTestSqlserver3Model> _notifiedValues = new List<TeardownTestSqlserver3Model>();

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
            string naming = "TestTeardown3";
            SqlTableDependency<TeardownTestSqlserver3Model> tableDependency = null;
            string dataBaseObjectsNamingConvention = null;

            var mapper = new ModelToTableMapper<TeardownTestSqlserver3Model>();
            mapper.AddMapping(c => c.Description, "Long Description");

            ////////////////////////////////////////////////////////
            // First Round
            ////////////////////////////////////////////////////////
            try
            {
                tableDependency = new SqlTableDependency<TeardownTestSqlserver3Model>(ConnectionString, mapper: mapper, teardown: false, namingForObjectsAlreadyExisting: naming);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                dataBaseObjectsNamingConvention = tableDependency.DataBaseObjectsNamingConvention;
                Assert.AreEqual(dataBaseObjectsNamingConvention, naming);

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(10000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_notifiedValues.Count, 3);
            Assert.AreEqual(_insertedValues[0].Id, _notifiedValues[0].Id);
            Assert.AreEqual(_insertedValues[0].Name, _notifiedValues[0].Name);
            Assert.AreEqual(_insertedValues[0].Description, _notifiedValues[0].Description);
            Assert.AreEqual(_insertedValues[1].Id, _notifiedValues[1].Id);
            Assert.AreEqual(_insertedValues[1].Name, _notifiedValues[1].Name);
            Assert.AreEqual(_insertedValues[1].Description, _notifiedValues[1].Description);
            Assert.AreEqual(_insertedValues[2].Id, _notifiedValues[2].Id);
            Assert.AreEqual(_insertedValues[2].Name, _notifiedValues[2].Name);
            Assert.AreEqual(_insertedValues[2].Description, _notifiedValues[2].Description);
            Assert.IsFalse(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, naming));

            ////////////////////////////////////////////////////////
            // Inserd data while sql table dependency is not running
            ////////////////////////////////////////////////////////
            
            ModifyTableContent();

            ////////////////////////////////////////////////////////
            // Second Round
            ////////////////////////////////////////////////////////

            try
            {
                tableDependency.Start();
                dataBaseObjectsNamingConvention = tableDependency.DataBaseObjectsNamingConvention;
                Assert.AreEqual(dataBaseObjectsNamingConvention, naming);

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(10000);
            }
            finally
            {
                tableDependency.Dispose();
            }

            Assert.AreEqual(_notifiedValues.Count, 9);

            Assert.AreEqual(_insertedValues[3].Id, _notifiedValues[3].Id);
            Assert.AreEqual(_insertedValues[3].Name, _notifiedValues[3].Name);
            Assert.AreEqual(_insertedValues[3].Description, _notifiedValues[3].Description);
            Assert.AreEqual(_insertedValues[4].Id, _notifiedValues[4].Id);
            Assert.AreEqual(_insertedValues[4].Name, _notifiedValues[4].Name);
            Assert.AreEqual(_insertedValues[4].Description, _notifiedValues[4].Description);
            Assert.AreEqual(_insertedValues[5].Id, _notifiedValues[5].Id);
            Assert.AreEqual(_insertedValues[5].Name, _notifiedValues[5].Name);
            Assert.AreEqual(_insertedValues[5].Description, _notifiedValues[5].Description);

            Assert.AreEqual(_insertedValues[6].Id, _notifiedValues[6].Id);
            Assert.AreEqual(_insertedValues[6].Name, _notifiedValues[6].Name);
            Assert.AreEqual(_insertedValues[6].Description, _notifiedValues[6].Description);
            Assert.AreEqual(_insertedValues[7].Id, _notifiedValues[7].Id);
            Assert.AreEqual(_insertedValues[7].Name, _notifiedValues[7].Name);
            Assert.AreEqual(_insertedValues[7].Description, _notifiedValues[7].Description);
            Assert.AreEqual(_insertedValues[8].Id, _notifiedValues[8].Id);
            Assert.AreEqual(_insertedValues[8].Name, _notifiedValues[8].Name);
            Assert.AreEqual(_insertedValues[8].Description, _notifiedValues[8].Description);

            Assert.IsFalse(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, naming));

            tableDependency = new SqlTableDependency<TeardownTestSqlserver3Model>(ConnectionString, mapper: mapper, teardown: true, namingForObjectsAlreadyExisting: naming);
            tableDependency.OnChanged += TableDependency_Changed;
            tableDependency.Start();
            Thread.Sleep(5000);

            tableDependency.Dispose();
            Thread.Sleep(1 * 60 * 1000);

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<TeardownTestSqlserver3Model> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _notifiedValues.Add(new TeardownTestSqlserver3Model() { Id = e.Entity.Id, Name = e.Entity.Name, Description = e.Entity.Description });
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
                    _insertedValues.Add(new TeardownTestSqlserver3Model { Id = _counter, Name = "Christian", Description = "Del Bianco" });
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name], [Long Description]) VALUES ({_counter}, 'Christian', 'Del Bianco')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    _counter++;

                    _insertedValues.Add(new TeardownTestSqlserver3Model { Id = _counter, Name = "Valentina", Description = "Del Bianco" });
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name], [Long Description]) VALUES ({_counter}, 'Valentina', 'Del Bianco')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    _counter++;

                    _insertedValues.Add(new TeardownTestSqlserver3Model { Id = _counter, Name = "Aurelia", Description = "Bezerra" });
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name], [Long Description]) VALUES ({_counter}, 'Aurelia', 'Bezerra')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    _counter++;
                }
            }
        }
    }
}