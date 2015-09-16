using System;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.DataAccess.Client;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.OracleClient.IntegrationTest.Model;
using TableDependency.OracleClient.IntegrationTest.Helpers;

namespace TableDependency.OracleClient.IntegrationTest
{
    [TestClass]
    public class LoadAndCount
    {
        private static string _connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private static string _tableName = ConfigurationManager.AppSettings.Get("tableName");
        private static int _counter = 0;

        [TestInitialize]
        public void TestInitialize()
        {
            using (var connection = new OracleConnection(_connectionString))
            {
                connection.Open();
                using (var sqlCommand = connection.CreateCommand())
                {
                    sqlCommand.CommandText = "DELETE FROM " + _tableName;
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestMethod]
        public void LoadAndCountTest()
        {
            var cts = new CancellationTokenSource(TimeSpan.FromMinutes(10));
            var token = cts.Token;

            var counterUpTo = 1000;
            var mapper = new ModelToTableMapper<Item>();
            mapper.AddMapping(c => c.Description, "Long Description");

            var listenerTask = Task.Factory.StartNew(() => new Listener(_connectionString, _tableName, mapper).Run(counterUpTo, token), token);
            Thread.Sleep(3000);

            using (var sqlConnection = new OracleConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    while (!listenerTask.IsCompleted)
                    {
                        if (_counter <= counterUpTo)
                        {
                            _counter++;
                            sqlCommand.CommandText = 
                                $"INSERT INTO {_tableName} (ID, NAME, \"Long Description\") VALUES ('{_counter}', '{DateTime.Now.Ticks}', 'Ticks {DateTime.Now.Ticks}')";
                            sqlCommand.ExecuteNonQuery();                            
                        }

                        Thread.Sleep(250);
                    }
                }
            }

            Assert.IsTrue(listenerTask.Result != null);
            Assert.IsTrue(listenerTask.Result.Counter == counterUpTo);
            Assert.IsTrue(!listenerTask.Result.SequentialNotificationFailed);
            Assert.IsTrue(Helper.AreAllDbObjectDisposed(_connectionString, listenerTask.Result.ObjectNaming));
        }
    }

    public class ListenerResult
    {
        public int Counter { get; set; }
        public string ObjectNaming { get; set; }
        public bool SequentialNotificationFailed { get; set; }
    }

    public class Listener
    {
        readonly OracleTableDependency<Item> _tableDependency;
        readonly ListenerResult _listenerResult = new ListenerResult();

        public string ObjectNaming { get; private set; }

        public Listener(string connectionString, string tableName, ModelToTableMapper<Item> mapper)
        {
            _tableDependency = new OracleTableDependency<Item>(connectionString, tableName, mapper);
            _tableDependency.OnChanged += TableDependency_OnChanged;
            _tableDependency.Start(60, 120);
            _listenerResult.ObjectNaming = _tableDependency.DataBaseObjectsNamingConvention;
        }

        private void TableDependency_OnChanged(object sender, RecordChangedEventArgs<Item> e)
        {
            _listenerResult.Counter = _listenerResult.Counter + 1;
            if (_listenerResult.Counter != e.Entity.Id)
            {
                _listenerResult.SequentialNotificationFailed = true;
            }
        }

        public ListenerResult Run(int countUpTo, CancellationToken token)
        {
            while (_listenerResult.Counter <= (countUpTo - 1))
            {
                Thread.Sleep(100);
                token.ThrowIfCancellationRequested();
            }

            _tableDependency.Stop();
            Thread.Sleep(5000);
            return _listenerResult;
        }
    }
}