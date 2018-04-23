using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.SqlServer;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    [Table("Item", Schema = "Transaction")]
    public class TransactionItem
    {
        public Guid TransactionItemId { get; set; }
        public string Description { get; set; }
    }

    [TestClass]
    public class UseSchemaOtherThanDboTestSqlServer2
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServer2008 sa"].ConnectionString;
        private const string TableName = "Item";
        private const string SchemaName = "Transaction";
        private static int _counter;
        private static readonly Dictionary<string, Tuple<TransactionItem, TransactionItem>> CheckValues = new Dictionary<string, Tuple<TransactionItem, TransactionItem>>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF NOT EXISTS(SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{SchemaName}') BEGIN EXEC sp_executesql N'CREATE SCHEMA [{SchemaName}];'; END;";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{TableName}' AND TABLE_SCHEMA = '{SchemaName}'";
                    var exists = (int)sqlCommand.ExecuteScalar();
                    if (exists > 0)
                    {
                        sqlCommand.CommandText = $"DROP TABLE [{SchemaName}].[{TableName}]";
                        sqlCommand.ExecuteNonQuery();
                    }

                    sqlCommand.CommandText = $"CREATE TABLE [{SchemaName}].[{TableName}] (TransactionItemId uniqueidentifier NULL, Description nvarchar(50) NOT NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{TableName}' AND TABLE_SCHEMA = '{SchemaName}'";
                    var exists = (int)sqlCommand.ExecuteScalar();
                    if (exists > 0)
                    {
                        sqlCommand.CommandText = $"DROP TABLE [{SchemaName}].[{TableName}]";
                        sqlCommand.ExecuteNonQuery();

                        sqlCommand.CommandText = $"DROP SCHEMA [{SchemaName}];";
                        sqlCommand.ExecuteNonQuery();
                    }
                }
            }
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TableWithTest()
        {
            SqlTableDependency<TransactionItem> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<TransactionItem>(ConnectionString);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 3);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Description, CheckValues[ChangeType.Insert.ToString()].Item1.Description);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Description, CheckValues[ChangeType.Update.ToString()].Item1.Description);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Description, CheckValues[ChangeType.Delete.ToString()].Item1.Description);
            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(naming));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<TransactionItem> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.Description = e.Entity.Description;
                    break;
                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.Description = e.Entity.Description;
                    break;
                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.Description = e.Entity.Description;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<TransactionItem, TransactionItem>(new TransactionItem { Description = "Christian" }, new TransactionItem()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<TransactionItem, TransactionItem>(new TransactionItem { Description = "Velia" }, new TransactionItem()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<TransactionItem, TransactionItem>(new TransactionItem { Description = "Velia" }, new TransactionItem()));

            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{SchemaName}].[{TableName}] ([Description]) VALUES ('{CheckValues[ChangeType.Insert.ToString()].Item1.Description}')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"UPDATE [{SchemaName}].[{TableName}] SET [Description] = '{CheckValues[ChangeType.Update.ToString()].Item1.Description}'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"DELETE FROM [{SchemaName}].[{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);
                }
            }
        }
    }
}