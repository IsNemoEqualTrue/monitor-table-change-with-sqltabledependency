using System.Configuration;
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
        private const string TableName = "Customer";

        [TestMethod]
        public void TaskCancellationTest()
        {
            string naming = null;
            SqlTableDependency<Customer> tableDependency = null;

            try
            {
                var mapper = new ModelToTableMapper<Customer>();
                mapper.AddMapping(c => c.Name, "First Name").AddMapping(c => c.Surname, "Second Name");

                tableDependency = new SqlTableDependency<Customer>(_connectionString, TableName, mapper);
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

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Customer> e)
        {
        }
    }
}