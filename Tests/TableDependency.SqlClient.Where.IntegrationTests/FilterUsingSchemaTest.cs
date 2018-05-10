using System;
using System.Collections.Generic;
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
    public class FilterUsingSchemaTest : SqlTableDependencyBaseTest
    {
        private class FilterUsingSchemaTestModel
        {
            public int? Id { get; set; }
            public string Name { get; set; }
        }

        // Cannot be static !!!
        private const int _id = 2;
        private static readonly string TableName = typeof(FilterUsingSchemaTestModel).Name;
        private const string SchemaName = "Zuzza";
        private static int _counter;
        private static readonly Dictionary<string, Tuple<FilterUsingSchemaTestModel, FilterUsingSchemaTestModel>> CheckValues = new Dictionary<string, Tuple<FilterUsingSchemaTestModel, FilterUsingSchemaTestModel>>();
        private static readonly Dictionary<string, Tuple<FilterUsingSchemaTestModel, FilterUsingSchemaTestModel>> CheckValuesOld = new Dictionary<string, Tuple<FilterUsingSchemaTestModel, FilterUsingSchemaTestModel>>();

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

                    sqlCommand.CommandText = $"CREATE TABLE [{SchemaName}].[{TableName}] (Id INT NULL, NAME nvarchar(50) NULL)";
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

            CheckValues.Clear();
            CheckValuesOld.Clear();

            _counter = 0;

            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<FilterUsingSchemaTestModel, FilterUsingSchemaTestModel>(new FilterUsingSchemaTestModel { Name = "Christian" }, new FilterUsingSchemaTestModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<FilterUsingSchemaTestModel, FilterUsingSchemaTestModel>(new FilterUsingSchemaTestModel { Name = "Velia" }, new FilterUsingSchemaTestModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<FilterUsingSchemaTestModel, FilterUsingSchemaTestModel>(new FilterUsingSchemaTestModel { Name = "Velia" }, new FilterUsingSchemaTestModel()));

            CheckValuesOld.Add(ChangeType.Insert.ToString(), new Tuple<FilterUsingSchemaTestModel, FilterUsingSchemaTestModel>(new FilterUsingSchemaTestModel { Name = "Christian" }, new FilterUsingSchemaTestModel()));
            CheckValuesOld.Add(ChangeType.Update.ToString(), new Tuple<FilterUsingSchemaTestModel, FilterUsingSchemaTestModel>(new FilterUsingSchemaTestModel { Name = "Velia" }, new FilterUsingSchemaTestModel()));
            CheckValuesOld.Add(ChangeType.Delete.ToString(), new Tuple<FilterUsingSchemaTestModel, FilterUsingSchemaTestModel>(new FilterUsingSchemaTestModel { Name = "Velia" }, new FilterUsingSchemaTestModel()));
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
                    }

                    sqlCommand.CommandText = $"DROP SCHEMA [{SchemaName}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestMethod]
        public void Test()
        {
            SqlTableDependency<FilterUsingSchemaTestModel> tableDependency = null;
            string naming;

            Expression<Func<FilterUsingSchemaTestModel, bool>> expression = p => p.Id == _id;
            ITableDependencyFilter filterExpression = new SqlTableDependencyFilter<FilterUsingSchemaTestModel>(expression);

            try
            {
                tableDependency = new SqlTableDependency<FilterUsingSchemaTestModel>(
                    ConnectionStringForSa,
                    includeOldValues: false,
                    tableName: TableName,
                    schemaName: SchemaName,
                    filter: filterExpression);

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
            Assert.IsNull(CheckValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, CheckValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.IsNull(CheckValuesOld[ChangeType.Update.ToString()]);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.IsNull(CheckValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        [TestMethod]
        public void TestWithOldValues()
        {
            SqlTableDependency<FilterUsingSchemaTestModel> tableDependency = null;
            string naming;

            Expression<Func<FilterUsingSchemaTestModel, bool>> expression = p => p.Id == _id;
            ITableDependencyFilter filterExpression = new SqlTableDependencyFilter<FilterUsingSchemaTestModel>(expression);

            try
            {
                tableDependency = new SqlTableDependency<FilterUsingSchemaTestModel>(
                    ConnectionStringForSa,
                    includeOldValues: true,
                    tableName: TableName,
                    schemaName: SchemaName,
                    filter: filterExpression);

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
            Assert.IsNull(CheckValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, CheckValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(CheckValuesOld[ChangeType.Update.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item2.Name);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.IsNull(CheckValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<FilterUsingSchemaTestModel> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;

                    if (e.EntityOldValues != null)
                    {
                        CheckValuesOld[ChangeType.Insert.ToString()].Item2.Name = e.EntityOldValues.Name;
                    }
                    else
                    {
                        CheckValuesOld[ChangeType.Insert.ToString()] = null;
                    }

                    break;

                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;

                    if (e.EntityOldValues != null)
                    {
                        CheckValuesOld[ChangeType.Update.ToString()].Item2.Name = e.EntityOldValues.Name;
                    }
                    else
                    {
                        CheckValuesOld[ChangeType.Update.ToString()] = null;
                    }

                    break;

                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;

                    if (e.EntityOldValues != null)
                    {
                        CheckValuesOld[ChangeType.Delete.ToString()].Item2.Name = e.EntityOldValues.Name;
                    }
                    else
                    {
                        CheckValuesOld[ChangeType.Delete.ToString()] = null;
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
                    sqlCommand.CommandText = $"INSERT INTO [{SchemaName}].[{TableName}] ([Id], [Name]) VALUES (1, 'Valentina')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{SchemaName}].[{TableName}] ([Id], [Name]) VALUES (2, '{CheckValues[ChangeType.Insert.ToString()].Item1.Name}')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{SchemaName}].[{TableName}] SET [Name] = 'Aurelia' WHERE Id = 1";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{SchemaName}].[{TableName}] SET [Name] = '{CheckValues[ChangeType.Update.ToString()].Item1.Name}' WHERE Id = 2";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{SchemaName}].[{TableName}] WHERE Id = 1";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{SchemaName}].[{TableName}] WHERE Id = 2";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}