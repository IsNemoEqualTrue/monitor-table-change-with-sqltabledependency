using System;
using System.Data.SqlClient;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Abstracts;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient.BaseTests;

namespace TableDependency.SqlClient.Where.IntegrationTests
{
    [TestClass]
    public class DateTimeTest : SqlTableDependencyBaseTest
    {
        private class TestDateTimeSqlServerModel
        {
            public int Id { get; set; }
            public DateTime Start { get; set; }
        }

        private int _insertedId;
        private int _deletedId;
        private readonly DateTime _now = DateTime.Now;
        private static readonly string TableName = typeof(TestDateTimeSqlServerModel).Name;
        private static int _counter;

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

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [int] NOT NULL, [Start] [datetime] NULL)";
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

        [TestMethod]
        public void Test()
        {
            SqlTableDependency<TestDateTimeSqlServerModel> tableDependency = null;
            string naming;

            Expression<Func<TestDateTimeSqlServerModel, bool>> expression = p => p.Start >= _now;
            ITableDependencyFilter filterExpression = new SqlTableDependencyFilter<TestDateTimeSqlServerModel>(expression);

            try
            {
                tableDependency = new SqlTableDependency<TestDateTimeSqlServerModel>(ConnectionStringForTestUser, filter: filterExpression);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 2);
            Assert.AreEqual(1, _insertedId);
            Assert.AreEqual(1, _deletedId);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<TestDateTimeSqlServerModel> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _insertedId = e.Entity.Id;

                    break;
                case ChangeType.Delete:
                    _deletedId = e.Entity.Id;
                    break;
            }
        }

        private void ModifyTableContent()
        {
            var yesterday = DateTime.Now.AddDays(-3);

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Start]) VALUES (1, @today)";
                    sqlCommand.Parameters.AddWithValue("@today", _now);
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.Parameters.Clear();
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Start]) VALUES (2, @yesterday)";
                    sqlCommand.Parameters.AddWithValue("@yesterday", yesterday);
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE from [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}