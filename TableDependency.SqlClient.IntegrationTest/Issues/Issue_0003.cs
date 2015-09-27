using System.Collections.Generic;
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
    public class Issue_0003
    {
        private static string _connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private const string TableName = "Issue0003";
        private int _counter;

        [TestInitialize]
        public void TestInitialize()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText =
                        $"IF OBJECT_ID('{TableName}', 'U') IS NULL BEGIN CREATE TABLE [{TableName}](" +
                        "[Id][int] IDENTITY(1, 1) NOT NULL," +
                        "[FirstName] [varchar](4000) NOT NULL," +
                        "[SecondName] [nvarchar](4000) NOT NULL," +
                        "[NotManagedColumnBecauseIsVarcharMAX] [nvarchar](MAX) NULL," +
                        "[NotManagedColumnBecauseIsXml] XML NULL) END;";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"DELETE FROM {TableName}";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestMethod]
        public void DealWithUnmanagedColumnsTypeTest()
        {
            SqlTableDependency<Issue_0003_Model> tableDependency = null;
            var interestedColumnsList = new List<string>() { "FirstName", "SecondName" };

            try
            {
                tableDependency = new SqlTableDependency<Issue_0003_Model>(_connectionString, TableName, updateOf: interestedColumnsList);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.Start();

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.IsTrue(this._counter == 3);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<Issue_0003_Model> e)
        {
            this._counter++;
        }

        private static void ModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([FirstName],[SecondName],[NotManagedColumnBecauseIsVarcharMAX]) VALUES ('Valentina', 'Del Bianco', 'Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Donec quam felis, ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis enim.')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [FirstName] = 'ntina'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [NotManagedColumnBecauseIsVarcharMAX] = 'Valentina Del Bianco'";
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