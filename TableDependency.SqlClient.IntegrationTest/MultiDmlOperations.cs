using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.SqlClient.IntegrationTest.Model;

namespace TableDependency.SqlClient.IntegrationTest
{
    [TestClass]
    public class MultiDmlOperations
    {
        private static string _connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private const string TableName = "MultiDmlOperations";
        private static List<Check_Model> _checkValues = new List<Check_Model>();

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
                        "[Id] [int] IDENTITY(1, 1) NOT NULL, " +
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

                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('CHRISTIAN', 'DEL BIANCO')";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('VELIA', 'CECCARELLI')";
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
        public void MultiDeleteTest()
        {
            _checkValues.Clear();
            SqlTableDependency<Check_Model> tableDependency = null;

            try
            {
                var mapper = new ModelToTableMapper<Check_Model>();
                mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second Name");

                tableDependency = new SqlTableDependency<Check_Model>(_connectionString, TableName, mapper);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.OnError += TableDependency_OnError;
                tableDependency.Start();

                Thread.Sleep(5000);

                var t = new Task(MultiDeleteOperation);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual("CHRISTIAN", _checkValues[1].Name);
            Assert.AreEqual("DEL BIANCO", _checkValues[1].Surname);
            Assert.AreEqual("VELIA", _checkValues[0].Name);
            Assert.AreEqual("CECCARELLI", _checkValues[0].Surname);
        }

        [TestMethod]
        public void MultiUpdateTest()
        {
            _checkValues.Clear();
            SqlTableDependency<Check_Model> tableDependency = null;

            try
            {
                var mapper = new ModelToTableMapper<Check_Model>();
                mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second Name");

                tableDependency = new SqlTableDependency<Check_Model>(_connectionString, TableName, mapper);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.OnError += TableDependency_OnError;
                tableDependency.Start();

                Thread.Sleep(10000);

                var t = new Task(MultiUpdateOperation);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual("AAA", _checkValues[1].Name);
            Assert.AreEqual("DEL BIANCO", _checkValues[1].Surname);
            Assert.AreEqual("AAA", _checkValues[0].Name);
            Assert.AreEqual("CECCARELLI", _checkValues[0].Surname);
        }

        [TestMethod]
        public void MultiInsertTest()
        {
            _checkValues.Clear();
            SqlTableDependency<Check_Model> tableDependency = null;

            try
            {
                var mapper = new ModelToTableMapper<Check_Model>();
                mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second Name");

                tableDependency = new SqlTableDependency<Check_Model>(_connectionString, TableName, mapper);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.OnError += TableDependency_OnError;
                tableDependency.Start();

                Thread.Sleep(10000);

                var t = new Task(MultiInsertOperation);
                t.Start();
                t.Wait(40000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual("NONNA", _checkValues[1].Name);
            Assert.AreEqual("DIRCE", _checkValues[1].Surname);
            Assert.AreEqual("ZIA", _checkValues[0].Name);
            Assert.AreEqual("ALFREDINA", _checkValues[0].Surname);
        }

        private void TableDependency_OnError(object sender, ErrorEventArgs e)
        {
            throw e.Error;
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<Check_Model> e)
        {
            _checkValues.Add(new Check_Model { Name = e.Entity.Name, Surname = e.Entity.Surname });
        }

        private static void MultiInsertOperation()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('NONNA', 'DIRCE'), ('ZIA', 'ALFREDINA')";
                    sqlCommand.ExecuteNonQuery();
                }
            }
            Thread.Sleep(500);
        }

        private static void MultiDeleteOperation()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);
                }
            }
        }

        private static void MultiUpdateOperation()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = 'AAA'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);
                }
            }
        }
    }
}