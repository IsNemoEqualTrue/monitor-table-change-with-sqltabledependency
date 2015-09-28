using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Mappers;
using TableDependency.SqlClient.IntegrationTest.Model;
using TableDependency.EventArgs;
using TableDependency.SqlClient.IntegrationTest.Helpers;

namespace TableDependency.SqlClient.IntegrationTest
{
    [TestClass]
    public class LoadAndCount
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private static string _tableName = "TestTable";
        private int _counter = 1;

        [TestInitialize]
        public void TestInitialize()
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText =
                        $"IF OBJECT_ID('{_tableName}', 'U') IS NULL BEGIN CREATE TABLE [{_tableName}]( " +
                        "[Id][int] IDENTITY(1, 1) NOT NULL, " +
                        "[First Name] [nvarchar](50) NOT NULL, " +
                        "[Second Name] [nvarchar](50) NOT NULL, " +
                        "[Born] [datetime] NULL); END";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"DELETE FROM [{_tableName}]";
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
            var mapper = new ModelToTableMapper<TestTable>();
            mapper.AddMapping(c => c.Name, "First Name").AddMapping(c => c.Surname, "Second Name");
            var listenerTask = Task.Factory.StartNew(() => new Listener(ConnectionString, _tableName, mapper).Run(counterUpTo, token), token);
            Thread.Sleep(3000);

            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    while (!listenerTask.IsCompleted)
                    {
                        if (_counter <= counterUpTo)
                        {
                            sqlCommand.CommandText = $"INSERT INTO [{_tableName}] ([First Name], [Second Name]) VALUES ('{DateTime.Now.Ticks}', '{_counter}')";
                            sqlCommand.ExecuteNonQuery();
                            _counter++;
                        }
                        
                        Thread.Sleep(250);
                    }
                }
            }

            Assert.IsTrue(listenerTask.Result != null);
            Assert.IsTrue(listenerTask.Result.Counter == counterUpTo);
            Assert.IsTrue(!listenerTask.Result.SequentialNotificationFailed);
            Assert.IsTrue(Helper.AreAllDbObjectDisposed(ConnectionString, listenerTask.Result.ObjectNaming));
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
        readonly SqlTableDependency<TestTable> _tableDependency;
        readonly ListenerResult _listenerResult = new ListenerResult();

        public string ObjectNaming{ get; private set; }

        public Listener(string connectionString, string tableName, ModelToTableMapper<TestTable> mapper)
        {
            _tableDependency = new SqlTableDependency<TestTable>(connectionString, tableName, mapper);
            _tableDependency.OnChanged += TableDependency_OnChanged;
            _tableDependency.Start(60, 120);
            _listenerResult.ObjectNaming = _tableDependency.DataBaseObjectsNamingConvention;
        }

        private void TableDependency_OnChanged(object sender, RecordChangedEventArgs<TestTable> e)
        {
            _listenerResult.Counter = _listenerResult.Counter + 1;
            if (_listenerResult.Counter.ToString() != e.Entity.Surname)
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