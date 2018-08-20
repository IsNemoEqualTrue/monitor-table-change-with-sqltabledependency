using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class UseSchemaOtherThanDboTestSqlServer : Base.SqlTableDependencyBaseTest
    {
        [Table("Customers", Schema = "test_schema")]
        private class UseSchemaOtherThanDboTestSqlServerModel
        {
            public string Name { get; set; }
        }

        private const string TableName = "Customers";
        private const string SchemaName = "test_schema";
        private static int _counter;
        private static readonly Dictionary<string, Tuple<UseSchemaOtherThanDboTestSqlServerModel, UseSchemaOtherThanDboTestSqlServerModel>> CheckValues = new Dictionary<string, Tuple<UseSchemaOtherThanDboTestSqlServerModel, UseSchemaOtherThanDboTestSqlServerModel>>();

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

                    sqlCommand.CommandText = $"CREATE TABLE [{SchemaName}].[{TableName}] ([Name] [nvarchar](50) NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
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
                        sqlCommand.CommandText = $"DROP TABLE [{SchemaName}].[{TableName}];";
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
            SqlTableDependency<UseSchemaOtherThanDboTestSqlServerModel> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<UseSchemaOtherThanDboTestSqlServerModel>(ConnectionStringForSa);
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
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, CheckValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming)== 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<UseSchemaOtherThanDboTestSqlServerModel> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    break;
                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                    break;
                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<UseSchemaOtherThanDboTestSqlServerModel, UseSchemaOtherThanDboTestSqlServerModel>(new UseSchemaOtherThanDboTestSqlServerModel { Name = "Christian" }, new UseSchemaOtherThanDboTestSqlServerModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<UseSchemaOtherThanDboTestSqlServerModel, UseSchemaOtherThanDboTestSqlServerModel>(new UseSchemaOtherThanDboTestSqlServerModel { Name = "Velia" }, new UseSchemaOtherThanDboTestSqlServerModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<UseSchemaOtherThanDboTestSqlServerModel, UseSchemaOtherThanDboTestSqlServerModel>(new UseSchemaOtherThanDboTestSqlServerModel { Name = "Velia" }, new UseSchemaOtherThanDboTestSqlServerModel()));

            using (var sqlConnection = new SqlConnection(ConnectionStringForSa))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO {SchemaName}.{TableName} ([Name]) VALUES ('{CheckValues[ChangeType.Insert.ToString()].Item1.Name}')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE {SchemaName}.{TableName} SET [Name] = '{CheckValues[ChangeType.Update.ToString()].Item1.Name}'";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM {SchemaName}.{TableName}";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}