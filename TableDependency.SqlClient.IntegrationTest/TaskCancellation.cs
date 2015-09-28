using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.SqlClient.IntegrationTest.Helpers;
using TableDependency.SqlClient.IntegrationTest.Model;

namespace TableDependency.SqlClient.IntegrationTest
{
    [TestClass]
    public class TaskCancellation
    {
        private static string _connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private const string TableName = "TestTable";

        [TestInitialize]
        public void TestInitialize()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText =
                        $"IF OBJECT_ID('{TableName}', 'U') IS NULL BEGIN CREATE TABLE [{TableName}]( " +
                        "[Id][int] IDENTITY(1, 1) NOT NULL, " +
                        "[First Name] [nvarchar](50) NOT NULL, " +
                        "[Second Name] [nvarchar](50) NOT NULL, " +
                        "[Born] [datetime] NULL); END";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestMethod]
        public void TaskCancellationTest()
        {
            string naming = null;
            SqlTableDependency<TestTable> tableDependency = null;

            try
            {
                var mapper = new ModelToTableMapper<TestTable>();
                mapper.AddMapping(c => c.Name, "First Name").AddMapping(c => c.Surname, "Second Name");

                tableDependency = new SqlTableDependency<TestTable>(_connectionString, TableName, mapper);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                tableDependency.Stop();

                Thread.Sleep(5000);
            }
            catch
            {
                tableDependency?.Dispose();
            }

            Assert.IsTrue(Helper.AreAllDbObjectDisposed(_connectionString, naming));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<TestTable> e)
        {
        }
    }
}