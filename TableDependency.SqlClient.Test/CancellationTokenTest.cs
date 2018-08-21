using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Base;

namespace TableDependency.SqlClient.Test
{
    public class CancellationTokenTestModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
        public DateTime Born { get; set; }
        public int Quantity { get; set; }
    }

    [TestClass]
    public class CancellationTokenTest : Base.SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(CancellationTokenTestModel).Name;

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
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

        [TestInitialize]
        public void TestInitialize()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void Test()
        {
            var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1));
            var token = cts.Token;

            var listenerSlq = new ListenerSlq(TableName);
            var objectNaming = listenerSlq.ObjectNaming;
            Task.Factory.StartNew(() => listenerSlq.Run(token), token);
            Thread.Sleep(1000 * 15 * 1);

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    while (token.IsCancellationRequested == false)
                    {
                        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{DateTime.Now.Ticks}', '{DateTime.Now.Ticks}')";
                        sqlCommand.ExecuteNonQuery();

                        Thread.Sleep(1000 * 1 * 1);
                    }
                }
            }

            listenerSlq.Dispose();
            listenerSlq = null;

            Thread.Sleep(1000 * 30 * 1);
            Assert.IsTrue(base.AreAllDbObjectDisposed(objectNaming));
            Assert.IsTrue(base.CountConversationEndpoints(objectNaming) == 0);
        }
    }

    public class ListenerSlq : Base.SqlTableDependencyBaseTest, IDisposable
    {
        private readonly SqlTableDependency<CancellationTokenTestModel> _tableDependency;    
        public string ObjectNaming { get; }

        public ListenerSlq(string tableName)
        {
            var mapper = new ModelToTableMapper<CancellationTokenTestModel>();
            mapper.AddMapping(c => c.Name, "First Name").AddMapping(c => c.Surname, "Second Name");

            _tableDependency = new SqlTableDependency<CancellationTokenTestModel>(ConnectionStringForTestUser, mapper: mapper);
            _tableDependency.OnChanged += (o, args) => { Debug.WriteLine("Received:" + args.Entity.Name); };
            _tableDependency.Start(60, 120);
            this.ObjectNaming = this._tableDependency.DataBaseObjectsNamingConvention;
        }

        public void Run(CancellationToken token)
        {
            while (token.IsCancellationRequested == false)
            {
                Thread.Sleep(1000 * 15 * 1);
            }
        }

        public void Dispose()
        {
            _tableDependency.Dispose();
        }
    }
}