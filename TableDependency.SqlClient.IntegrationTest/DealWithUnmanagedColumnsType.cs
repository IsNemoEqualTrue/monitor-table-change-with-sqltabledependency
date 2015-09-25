using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.EventArgs;
using TableDependency.SqlClient.IntegrationTest.Model;

namespace TableDependency.SqlClient.IntegrationTest
{
    [TestClass]
    public class DealWithUnmanagedColumnsType
    {
        private static string _connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private const string TableName = "NotManagedColumns";
        private int _counter = 0;

        [TestMethod]
        public void DealWithUnmanagedColumnsTypeTest()
        {
            SqlTableDependency<TableWithNotManagedColumns> tableDependency = null;
            var interestedColumnsList = new List<string>() { "FirstName", "SecondName" };

            try
            {
                tableDependency = new SqlTableDependency<TableWithNotManagedColumns>(_connectionString, TableName, columnsToMonitorDuringUpdate: interestedColumnsList);
                tableDependency.OnChanged += TableDependency_Changed;
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

            Assert.IsTrue(_counter == 3);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<TableWithNotManagedColumns> e)
        {
            _counter++;
        }

        private static void ModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [NotManagedColumns] ([FirstName],[SecondName],[ManagedColumnBecauseIsVarcharMAX]) VALUES ('Valentina', 'Del Bianco', 'Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Donec quam felis, ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis enim.')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = $"UPDATE [NotManagedColumns] SET [FirstName] = 'ntina'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = $"UPDATE [NotManagedColumns] SET [ManagedColumnBecauseIsVarcharMAX] = 'Valentina Del Bianco'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = "DELETE FROM [NotManagedColumns]";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);
                }
            }
        }
    }
}