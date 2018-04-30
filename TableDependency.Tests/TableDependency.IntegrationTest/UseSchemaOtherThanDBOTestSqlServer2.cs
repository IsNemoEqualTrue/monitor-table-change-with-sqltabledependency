using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Base;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    [Table("Item", Schema = "Transaction")]
    public class UseSchemaOtherThanDboTestSqlServer2Model
    {
        public Guid TransactionItemId { get; set; }
        public string Description { get; set; }
    }

    [TestClass]
    public class UseSchemaOtherThanDboTestSqlServer2 : SqlTableDependencyBaseTest
    {
        private const string TableName = "Item";
        private const string SchemaName = "Transaction";
        private static int _counter;
        private static readonly Dictionary<string, Tuple<UseSchemaOtherThanDboTestSqlServer2Model, UseSchemaOtherThanDboTestSqlServer2Model>> CheckValues = new Dictionary<string, Tuple<UseSchemaOtherThanDboTestSqlServer2Model, UseSchemaOtherThanDboTestSqlServer2Model>>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForSa))
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
            using (var sqlConnection = new SqlConnection(ConnectionStringForSa))
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
            SqlTableDependency<UseSchemaOtherThanDboTestSqlServer2Model> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<UseSchemaOtherThanDboTestSqlServer2Model>(ConnectionStringForSa);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 10 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 3);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Description, CheckValues[ChangeType.Insert.ToString()].Item1.Description);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Description, CheckValues[ChangeType.Update.ToString()].Item1.Description);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Description, CheckValues[ChangeType.Delete.ToString()].Item1.Description);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming)== 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<UseSchemaOtherThanDboTestSqlServer2Model> e)
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
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<UseSchemaOtherThanDboTestSqlServer2Model, UseSchemaOtherThanDboTestSqlServer2Model>(new UseSchemaOtherThanDboTestSqlServer2Model { Description = "Christian" }, new UseSchemaOtherThanDboTestSqlServer2Model()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<UseSchemaOtherThanDboTestSqlServer2Model, UseSchemaOtherThanDboTestSqlServer2Model>(new UseSchemaOtherThanDboTestSqlServer2Model { Description = "Velia" }, new UseSchemaOtherThanDboTestSqlServer2Model()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<UseSchemaOtherThanDboTestSqlServer2Model, UseSchemaOtherThanDboTestSqlServer2Model>(new UseSchemaOtherThanDboTestSqlServer2Model { Description = "Velia" }, new UseSchemaOtherThanDboTestSqlServer2Model()));

            using (var sqlConnection = new SqlConnection(ConnectionStringForSa))
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