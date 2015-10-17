using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers;
using TableDependency.IntegrationTest.Helpers.SqlServer;
using TableDependency.IntegrationTest.Models;
using TableDependency.Mappers;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    [TestClass]
    public class LoadAndCountTestSqlServer
    {
        private static readonly string _connectionString = ConfigurationManager.ConnectionStrings["SqlServerConnectionString"].ConnectionString;
        private static string TableName = "TestTable";
        private int _counter = 1;

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

                    sqlCommand.CommandText =
                        $"CREATE TABLE [{TableName}]( " +
                        "[Id][int] IDENTITY(1, 1) NOT NULL, " +
                        "[First Name] [nvarchar](50) NOT NULL, " +
                        "[Second Name] [nvarchar](50) NOT NULL, " +
                        "[Born] [datetime] NULL)";
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

        [TestMethod]
        public void LoadAndCountTest()
        {
            var cts = new CancellationTokenSource(TimeSpan.FromMinutes(10));
            var token = cts.Token;

            var counterUpTo = 1000;
            var mapper = new ModelToTableMapper<Check_Model>();
            mapper.AddMapping(c => c.Name, "First Name").AddMapping(c => c.Surname, "Second Name");
            var listenerTask = Task.Factory.StartNew(() => new ListenerSlq(_connectionString, TableName, mapper).Run(counterUpTo, token), token);
            Thread.Sleep(3000);

            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    while (!listenerTask.IsCompleted)
                    {
                        if (this._counter <= counterUpTo)
                        {
                            sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{DateTime.Now.Ticks}', '{this._counter}')";
                            sqlCommand.ExecuteNonQuery();
                            this._counter++;
                        }
                        
                        Thread.Sleep(250);
                    }
                }
            }

            Assert.IsTrue(listenerTask.Result != null);
            Assert.IsTrue(listenerTask.Result.Counter == counterUpTo);
            Assert.IsTrue(!listenerTask.Result.SequentialNotificationFailed);
            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(_connectionString, listenerTask.Result.ObjectNaming));
        }
    }

    public class ListenerResultSql
    {
        public int Counter { get; set; }
        public string ObjectNaming { get; set; }
        public bool SequentialNotificationFailed { get; set; }
    }

    public class ListenerSlq
    {
        readonly SqlTableDependency<Check_Model> _tableDependency;
        readonly ListenerResultSql _listenerResult = new ListenerResultSql();

        public string ObjectNaming{ get; private set; }

        public ListenerSlq(string connectionString, string tableName, ModelToTableMapper<Check_Model> mapper)
        {
            this._tableDependency = new SqlTableDependency<Check_Model>(connectionString, tableName, mapper);
            this._tableDependency.OnChanged += this.TableDependency_OnChanged;
            this._tableDependency.Start(60, 120);
            this._listenerResult.ObjectNaming = this._tableDependency.DataBaseObjectsNamingConvention;
        }

        private void TableDependency_OnChanged(object sender, RecordChangedEventArgs<Check_Model> e)
        {
            this._listenerResult.Counter = this._listenerResult.Counter + 1;
            if (this._listenerResult.Counter.ToString() != e.Entity.Surname)
            {
                this._listenerResult.SequentialNotificationFailed = true;
            }
        }

        public ListenerResultSql Run(int countUpTo, CancellationToken token)
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