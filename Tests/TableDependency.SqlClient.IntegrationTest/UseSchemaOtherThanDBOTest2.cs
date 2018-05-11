using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient.BaseTests;

namespace TableDependency.SqlClient.IntegrationTests
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
        private static Dictionary<string, Tuple<UseSchemaOtherThanDboTestSqlServer2Model, UseSchemaOtherThanDboTestSqlServer2Model>> _checkValues = new Dictionary<string, Tuple<UseSchemaOtherThanDboTestSqlServer2Model, UseSchemaOtherThanDboTestSqlServer2Model>>();
        private static Dictionary<string, Tuple<UseSchemaOtherThanDboTestSqlServer2Model, UseSchemaOtherThanDboTestSqlServer2Model>> _checkValuesOld = new Dictionary<string, Tuple<UseSchemaOtherThanDboTestSqlServer2Model, UseSchemaOtherThanDboTestSqlServer2Model>>();

        [ClassInitialize]
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

        [TestInitialize]
        public void TestInitialize()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForSa))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{SchemaName}].[{TableName}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }

            _checkValues.Clear();
            _checkValuesOld.Clear();

            _counter = 0;

            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<UseSchemaOtherThanDboTestSqlServer2Model, UseSchemaOtherThanDboTestSqlServer2Model>(new UseSchemaOtherThanDboTestSqlServer2Model { Description = "Christian" }, new UseSchemaOtherThanDboTestSqlServer2Model()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<UseSchemaOtherThanDboTestSqlServer2Model, UseSchemaOtherThanDboTestSqlServer2Model>(new UseSchemaOtherThanDboTestSqlServer2Model { Description = "Velia" }, new UseSchemaOtherThanDboTestSqlServer2Model()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<UseSchemaOtherThanDboTestSqlServer2Model, UseSchemaOtherThanDboTestSqlServer2Model>(new UseSchemaOtherThanDboTestSqlServer2Model { Description = "Velia" }, new UseSchemaOtherThanDboTestSqlServer2Model()));

            _checkValuesOld.Add(ChangeType.Insert.ToString(), new Tuple<UseSchemaOtherThanDboTestSqlServer2Model, UseSchemaOtherThanDboTestSqlServer2Model>(new UseSchemaOtherThanDboTestSqlServer2Model { Description = "Christian" }, new UseSchemaOtherThanDboTestSqlServer2Model()));
            _checkValuesOld.Add(ChangeType.Update.ToString(), new Tuple<UseSchemaOtherThanDboTestSqlServer2Model, UseSchemaOtherThanDboTestSqlServer2Model>(new UseSchemaOtherThanDboTestSqlServer2Model { Description = "Velia" }, new UseSchemaOtherThanDboTestSqlServer2Model()));
            _checkValuesOld.Add(ChangeType.Delete.ToString(), new Tuple<UseSchemaOtherThanDboTestSqlServer2Model, UseSchemaOtherThanDboTestSqlServer2Model>(new UseSchemaOtherThanDboTestSqlServer2Model { Description = "Velia" }, new UseSchemaOtherThanDboTestSqlServer2Model()));
        }
        
        [ClassCleanup]
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
        public void Test()
        {
            SqlTableDependency<UseSchemaOtherThanDboTestSqlServer2Model> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<UseSchemaOtherThanDboTestSqlServer2Model>(ConnectionStringForSa);
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

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Description, _checkValues[ChangeType.Insert.ToString()].Item1.Description);
            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Description, _checkValues[ChangeType.Update.ToString()].Item1.Description);
            Assert.IsNull(_checkValuesOld[ChangeType.Update.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Description, _checkValues[ChangeType.Delete.ToString()].Item1.Description);
            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming)== 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TestWithOldValues()
        {
            SqlTableDependency<UseSchemaOtherThanDboTestSqlServer2Model> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<UseSchemaOtherThanDboTestSqlServer2Model>(ConnectionStringForSa, includeOldValues: true);
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

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Description, _checkValues[ChangeType.Insert.ToString()].Item1.Description);
            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Description, _checkValues[ChangeType.Update.ToString()].Item1.Description);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.Description, _checkValues[ChangeType.Insert.ToString()].Item2.Description);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Description, _checkValues[ChangeType.Delete.ToString()].Item1.Description);
            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<UseSchemaOtherThanDboTestSqlServer2Model> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.Description = e.Entity.Description;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.Description = e.EntityOldValues.Description;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()] = null;
                    }

                    break;

                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.Description = e.Entity.Description;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.Description = e.EntityOldValues.Description;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Update.ToString()] = null;
                    }

                    break;

                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.Description = e.Entity.Description;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.Description = e.EntityOldValues.Description;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Delete.ToString()] = null;
                    }

                    break;
            }
        }

        private static void ModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForSa))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{SchemaName}].[{TableName}] ([Description]) VALUES ('{_checkValues[ChangeType.Insert.ToString()].Item1.Description}')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{SchemaName}].[{TableName}] SET [Description] = '{_checkValues[ChangeType.Update.ToString()].Item1.Description}'";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{SchemaName}].[{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}