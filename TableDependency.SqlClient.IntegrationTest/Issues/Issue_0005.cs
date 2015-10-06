using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.EventArgs;
using TableDependency.SqlClient.IntegrationTest.Model;

namespace TableDependency.SqlClient.IntegrationTest.Issues
{
    [TestClass]
    public class Issue_0005
    {
        private static string _connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private const string TableName = "Issue_0005";
        private int _counter;
        private decimal _version;
        private string _format;

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

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}](" +
                        "[MessageId][BIGINT] IDENTITY(1, 1) NOT NULL PRIMARY KEY," +
                        "[Format] [VARCHAR](500) NULL," +
                        "[Version] [NUMERIC](19, 0) NULL)";
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

        public TestContext TestContext { get; set; }

        [TestMethod]
        public void ProblemWithNumeric()
        {
            using (var sqlTableDependency = new SqlTableDependency<Issue_0005_Model>(_connectionString, TableName))
            {
                sqlTableDependency.OnChanged += this.SqlTableDependency_OnChanged;
                sqlTableDependency.OnError += this.SqlTableDependency_OnError;
                sqlTableDependency.Start();

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000);
            }

            Assert.IsTrue(this._counter == 4);
            Assert.IsTrue(this._format == "<names><name>Valentina Del Bianco</name></names>");
            Assert.IsTrue(this._version == 23554329);
        }

        private void SqlTableDependency_OnError(object sender, TableDependency.EventArgs.ErrorEventArgs e)
        {
            throw e.Error;
        }

        private void SqlTableDependency_OnChanged(object sender, RecordChangedEventArgs<Issue_0005_Model> e)
        {
            this._version = e.Entity.Version.GetValueOrDefault();
            this._format = e.Entity.Format;

            this.TestContext.WriteLine("Format: {0}, Version: {1}", e.Entity.Format, e.Entity.Version);

            this._counter++;
        }

        private static void ModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Format], [Version]) VALUES ('Valentina', 1)";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Version] = 23554329";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Format] = '<names><name>Valentina Del Bianco</name></names>'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);
                }
            }
        }
    }
}