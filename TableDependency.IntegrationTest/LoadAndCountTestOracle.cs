using System;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.DataAccess.Client;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.IntegrationTest.Models;
using TableDependency.Mappers;
using TableDependency.OracleClient;

namespace TableDependency.IntegrationTest
{
    [TestClass]
    public class LoadAndCountTestOracle
    {
        private static int _counter = 0;
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static readonly string TableName = "AAAA_Table".ToUpper();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            OracleHelper.DropTable(ConnectionString, TableName);
        }

        [TestInitialize()]
        public void TestInitialize()
        {
            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {TableName} (ID number(10), NAME varchar2(50), \"Long Description\" varchar2(4000))";
                    command.ExecuteNonQuery();
                }
            }
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            OracleHelper.DropTable(ConnectionString, TableName);
        }        

        [TestMethod]
        public void LoadAndCountTest()
        {
            var cts = new CancellationTokenSource(TimeSpan.FromMinutes(10));
            var token = cts.Token;

            var counterUpTo = 1000;
            var mapper = new ModelToTableMapper<Item>();
            mapper.AddMapping(c => c.Description, "Long Description");

            var listenerTask = Task.Factory.StartNew(() => new ListenerOrc(ConnectionString, TableName, mapper).Run(counterUpTo, token), token);
            Thread.Sleep(3000);

            using (var sqlConnection = new OracleConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    while (!listenerTask.IsCompleted)
                    {
                        if (_counter <= counterUpTo)
                        {
                            _counter++;
                            sqlCommand.CommandText = $"INSERT INTO {TableName} (ID, NAME, \"Long Description\") VALUES ({_counter}, '{DateTime.Now.Ticks}', 'Ticks {DateTime.Now.Ticks}')";
                            sqlCommand.ExecuteNonQuery();                            
                        }

                        Thread.Sleep(250);
                    }
                }
            }

            Assert.IsTrue(listenerTask.Result != null);
            Assert.IsTrue(listenerTask.Result.Counter == counterUpTo);
            Assert.IsTrue(!listenerTask.Result.SequentialNotificationFailed);
            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, listenerTask.Result.ObjectNaming));
        }
    }

    public class ListenerResultOrc
    {
        public int Counter { get; set; }
        public string ObjectNaming { get; set; }
        public bool SequentialNotificationFailed { get; set; }
    }

    public class ListenerOrc
    {
        readonly OracleTableDependency<Item> _tableDependency;
        readonly ListenerResultOrc _listenerResult = new ListenerResultOrc();

        public string ObjectNaming { get; private set; }

        public ListenerOrc(string connectionString, string tableName, ModelToTableMapper<Item> mapper)
        {
            this._tableDependency = new OracleTableDependency<Item>(connectionString, tableName, mapper);
            this._tableDependency.OnChanged += this.TableDependency_OnChanged;
            this._tableDependency.Start(60, 120);
            this._listenerResult.ObjectNaming = this._tableDependency.DataBaseObjectsNamingConvention;
        }

        private void TableDependency_OnChanged(object sender, RecordChangedEventArgs<Item> e)
        {
            this._listenerResult.Counter = this._listenerResult.Counter + 1;
            if (this._listenerResult.Counter != e.Entity.Id)
            {
                this._listenerResult.SequentialNotificationFailed = true;
            }
        }

        public ListenerResultOrc Run(int countUpTo, CancellationToken token)
        {
            while (this._listenerResult.Counter <= (countUpTo - 1))
            {
                Thread.Sleep(100);
                token.ThrowIfCancellationRequested();
            }

            this._tableDependency.Stop();
            Thread.Sleep(5000);
            return this._listenerResult;
        }
    }
}