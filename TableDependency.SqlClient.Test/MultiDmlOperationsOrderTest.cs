using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class MultiDmlOperationsOrderTest : Base.SqlTableDependencyBaseTest
    {
        private class MultiDmlOperationsOrderTestModel
        {
            public int Id { get; set; }
            public DateTime When { get; set; }
            public string Letter { get; set; }

            [NotMapped]
            public ChangeType ChangeType { get; set; }
        }

        private static readonly string TableName = typeof(MultiDmlOperationsOrderTestModel).Name;
        private static IList<MultiDmlOperationsOrderTestModel> _checkValues;

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

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [int] NULL, [Letter] NVARCHAR(50), [When] DATETIME NOT NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize]
        public void TestInitialize()
        {
            _checkValues =  new List<MultiDmlOperationsOrderTestModel>();

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
        public void MultiInsertTest1()
        { 
            SqlTableDependency<MultiDmlOperationsOrderTestModel> tableDependency = null;

            try
            {
                tableDependency = new SqlTableDependency<MultiDmlOperationsOrderTestModel>(ConnectionStringForTestUser, tableName: TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.OnError += this.TableDependency_OnError;
                tableDependency.Start();

                var t = new Task(MultiInsertOperation1);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(3, _checkValues.Count);
            Assert.IsTrue(_checkValues[0].Id == 100);
            Assert.IsTrue(_checkValues[1].Id == 200);
            Assert.IsTrue(_checkValues[2].Id == 300);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void MultiInsertTest2()
        {
            SqlTableDependency<MultiDmlOperationsOrderTestModel> tableDependency = null;

            try
            {
                tableDependency = new SqlTableDependency<MultiDmlOperationsOrderTestModel>(ConnectionStringForTestUser, tableName: TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.OnError += this.TableDependency_OnError;
                tableDependency.Start();

                var t = new Task(MultiInsertOperation2);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(3, _checkValues.Count);
            Assert.IsTrue(_checkValues[0].Letter == "a");
            Assert.IsTrue(_checkValues[1].Letter == "b");
            Assert.IsTrue(_checkValues[2].Letter == "c");
        }


        private void TableDependency_OnError(object sender, ErrorEventArgs e)
        {
            throw e.Error;
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<MultiDmlOperationsOrderTestModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues.Add(new MultiDmlOperationsOrderTestModel { Id = e.Entity.Id, When = e.Entity.When, Letter = e.Entity.Letter });
                    break;

                case ChangeType.Update:
                    throw new Exception("Update non expected");

                case ChangeType.Delete:
                    throw new Exception("Delete non expected");
            }
        }

        private static void MultiInsertOperation1()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                var sql = $"INSERT INTO [{TableName}] ([Id], [When]) VALUES" +
                          "(100, GETDATE())," +
                          "(200, GETDATE())," +
                          "(300, GETDATE())";

                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = sql;
                    sqlCommand.ExecuteNonQuery();
                }
            }

            Thread.Sleep(500);
        }

        private static void MultiInsertOperation2()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                var sql = $"INSERT INTO [{TableName}] ([When], [Letter]) VALUES" +
                          "(GETDATE(), 'a')," +
                          "(GETDATE(), 'b')," +
                          "(GETDATE(), 'c')";

                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = sql;
                    sqlCommand.ExecuteNonQuery();
                }
            }

            Thread.Sleep(500);
        }
    }
}