using System;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.ManagedDataAccess.Client;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.OracleClient;
using System.Collections.Generic;

namespace TableDependency.IntegrationTest
{
    public class StatusTestOracleModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
        public DateTime Born { get; set; }
        public int Quantity { get; set; }
    }

    [TestClass]
    public class StatusTestOracle
    {
        private OracleTableDependency<StatusTestOracleModel> _tableDependency = null;
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private const string TableName = "AAA_StatusCheckTest";
        private static IDictionary<TableDependencyStatus, bool> statuses = new Dictionary<TableDependencyStatus, bool>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {           
            OracleHelper.DropTable(ConnectionString, TableName);

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {TableName} (ID number(10), NAME varchar2(50), qty number(10))";
                    command.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize()]
        public void TestInitialize()
        {
            statuses.Add(TableDependencyStatus.Starting, false);
            statuses.Add(TableDependencyStatus.Started, false);
            statuses.Add(TableDependencyStatus.WaitingForNotification, false);
            statuses.Add(TableDependencyStatus.MessageReadyToBeNotified, false);
            statuses.Add(TableDependencyStatus.MessageSent, false);
            statuses.Add(TableDependencyStatus.StoppedDueToCancellation, false);
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            OracleHelper.DropTable(ConnectionString, TableName);
        }

        [TestCategory("Oracle")]
        [TestMethod]
        public void StatusTest()
        {
            try
            {
                this._tableDependency = new OracleTableDependency<StatusTestOracleModel>(ConnectionString, TableName);
                this._tableDependency.OnChanged += this.TableDependency_Changed;
                this._tableDependency.OnStatusChanged += _tableDependency_OnStatusChanged;

                this._tableDependency.Start();

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000);

                this._tableDependency.Stop();

                foreach (var status in statuses)
                {
                    Assert.IsTrue(statuses[status.Key] == true);
                }
            }
            finally
            {
                this._tableDependency?.Dispose();
            }
        }

        private void _tableDependency_OnStatusChanged(object sender, StatusChangedEventArgs e)
        {
            statuses[e.Status] = true;
            Assert.IsTrue(e.Status == this._tableDependency.Status);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<StatusTestOracleModel> e)
        {

        }

        private static void ModifyTableContent()
        {
            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"BEGIN INSERT INTO {TableName} (ID, NAME, QTY) VALUES (1, '2', '3'); END;";
                    command.ExecuteNonQuery();
                    Thread.Sleep(2000);

                    command.CommandText = $"BEGIN UPDATE {TableName} SET NAME = '1', QTY = '2'; END;";
                    command.ExecuteNonQuery();
                    Thread.Sleep(2000);

                    command.CommandText = $"BEGIN DELETE FROM {TableName}; END;";
                    command.ExecuteNonQuery();
                    Thread.Sleep(2000);
                }
            }
        }
    }
}