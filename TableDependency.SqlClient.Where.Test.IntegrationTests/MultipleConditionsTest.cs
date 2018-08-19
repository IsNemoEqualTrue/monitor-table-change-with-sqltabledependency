using System;
using System.Data.SqlClient;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Abstracts;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient.Test.Base;

namespace TableDependency.SqlClient.Where.Test.IntegrationTests
{
    [TestClass]
    public class MultipleConditionsTest : SqlTableDependencyBaseTest
    {
        private class ProdottiSqlServerModel
        {
            public int Id { get; set; }
            public int CategoryId { get; set; }
            public int ItemsInStock { get; set; }
        }

        private int _insertedId;
        private int _updatedId;
        private int _deletedId;

        private static readonly string TableName = typeof(ProdottiSqlServerModel).Name;
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

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [int] NOT NULL, [CategoryId] [int] NOT NULL, [Quantity] [int] NOT NULL)";
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
            SqlTableDependency<ProdottiSqlServerModel> tableDependency = null;
            string naming;

            var mapper = new ModelToTableMapper<ProdottiSqlServerModel>();
            mapper.AddMapping(c => c.ItemsInStock, "Quantity");

            Expression<Func<ProdottiSqlServerModel, bool>> expression = p => (p.CategoryId == 1 || p.CategoryId == 2) && p.ItemsInStock <= 10;
            ITableDependencyFilter whereCondition = new SqlTableDependencyFilter<ProdottiSqlServerModel>(expression, mapper);

            try
            {
                tableDependency = new SqlTableDependency<ProdottiSqlServerModel>(ConnectionStringForTestUser, mapper: mapper, filter: whereCondition);
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

            Assert.AreEqual(_counter, 3);
            Assert.AreEqual(_insertedId, 1);
            Assert.AreEqual(_updatedId, 2);
            Assert.AreEqual(_deletedId, 2);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<ProdottiSqlServerModel> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _insertedId = e.Entity.Id;
                    break;

                case ChangeType.Update:
                    _updatedId = e.Entity.Id;
                    break;

                case ChangeType.Delete:
                    _deletedId = e.Entity.Id;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    // Notification: YES
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [CategoryId], [Quantity]) VALUES (1, 1, 9)";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    // Notification: NO 
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [CategoryId], [Quantity]) VALUES (2, 2, 11)";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    // Notification: NO 
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [CategoryId], [Quantity]) VALUES (3, 3, 3)";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    // Notification: NO
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Quantity] = 19 WHERE [CategoryId] = 1";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    // Notification: YES
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Quantity] = 1 WHERE [CategoryId] = 2";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    // Notification: NO
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Quantity] = 1 WHERE [CategoryId] = 3";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    // Notification: NO
                    sqlCommand.CommandText = $"DELETE from [{TableName}] WHERE [CategoryId] = 1";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    // Notification: YES
                    sqlCommand.CommandText = $"DELETE from [{TableName}] WHERE [CategoryId] = 2";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    // Notification: NO
                    sqlCommand.CommandText = $"DELETE from [{TableName}] WHERE [CategoryId] = 3";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}
