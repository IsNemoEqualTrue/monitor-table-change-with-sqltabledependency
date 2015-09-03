using System.Configuration;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.OracleClient.IntegrationTest.Helpers;
using TableDependency.OracleClient.IntegrationTest.Model;

namespace TableDependency.OracleClient.IntegrationTest
{
    [TestClass]
    public class TaskCancellation
    {
        private static string ConnectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private static string TableName = ConfigurationManager.AppSettings.Get("tableName");

        [TestMethod]
        public void TaskCancellationTest()
        {
            string naming = null;
            OracleTableDependency<Item> tableDependency = null;

            try
            {
                var mapper = new ModelToTableMapper<Item>();
                mapper.AddMapping(c => c.Description, "Long Description");

                tableDependency = new OracleTableDependency<Item>(ConnectionString, TableName, mapper);
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

            Assert.IsTrue(Helper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Item> e)
        {
        }
    }
}