using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
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
    public class WithDataAnnotationTest : SqlTableDependencyBaseTest
    {
        [Table("FilterWithDataAnnotationModel", Schema = "Filter")]
        private class WithDataAnnotationTestModel
        {
            [Column("Id")]
            public int Identificator { get; set; }

            public string Name { get; set; }

            [Column("Last Name")]
            public string Surname { get; set; }
        }

        // Cannot be static !!!
        private const int _id = 2;
        private static readonly string TableName = "FilterWithDataAnnotationModel";
        private const string SchemaName = "Filter";
        private static int _counter;
        private static readonly Dictionary<string, Tuple<WithDataAnnotationTestModel, WithDataAnnotationTestModel>> CheckValues = new Dictionary<string, Tuple<WithDataAnnotationTestModel, WithDataAnnotationTestModel>>();
        private static readonly Dictionary<string, Tuple<WithDataAnnotationTestModel, WithDataAnnotationTestModel>> CheckValuesOld = new Dictionary<string, Tuple<WithDataAnnotationTestModel, WithDataAnnotationTestModel>>();

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

                    sqlCommand.CommandText =
                        $"CREATE TABLE [{SchemaName}].[{TableName}]( " +
                        "[Id] [int] NOT NULL, " +
                        "[Name] [nvarchar](50) NOT NULL, " +
                        "[Last Name] [nvarchar](50) NULL, " +
                        "[Born] [datetime] NULL)";

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

            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<WithDataAnnotationTestModel, WithDataAnnotationTestModel>(new WithDataAnnotationTestModel { Identificator = _id, Surname = "Del Bianco", Name = "Christian" }, new WithDataAnnotationTestModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<WithDataAnnotationTestModel, WithDataAnnotationTestModel>(new WithDataAnnotationTestModel { Identificator = _id, Surname = "Nonna", Name = "Velia" }, new WithDataAnnotationTestModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<WithDataAnnotationTestModel, WithDataAnnotationTestModel>(new WithDataAnnotationTestModel { Identificator = _id, Surname = "Nonna", Name = "Velia" }, new WithDataAnnotationTestModel()));

            CheckValuesOld.Add(ChangeType.Insert.ToString(), new Tuple<WithDataAnnotationTestModel, WithDataAnnotationTestModel>(new WithDataAnnotationTestModel { Identificator = _id, Surname = "Del Bianco", Name = "Christian" }, new WithDataAnnotationTestModel()));
            CheckValuesOld.Add(ChangeType.Update.ToString(), new Tuple<WithDataAnnotationTestModel, WithDataAnnotationTestModel>(new WithDataAnnotationTestModel { Identificator = _id, Surname = "Nonna", Name = "Velia" }, new WithDataAnnotationTestModel()));
            CheckValuesOld.Add(ChangeType.Delete.ToString(), new Tuple<WithDataAnnotationTestModel, WithDataAnnotationTestModel>(new WithDataAnnotationTestModel { Identificator = _id, Surname = "Nonna", Name = "Velia" }, new WithDataAnnotationTestModel()));
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

        [TestMethod]
        public void Test()
        {
            SqlTableDependency<WithDataAnnotationTestModel> tableDependency = null;
            string naming;

            Expression<Func<WithDataAnnotationTestModel, bool>> expression = p => p.Identificator == _id;
            ITableDependencyFilter filterExpression = new SqlTableDependencyFilter<WithDataAnnotationTestModel>(expression);

            try
            {
                tableDependency = new SqlTableDependency<WithDataAnnotationTestModel>(
                    ConnectionStringForSa,
                    includeOldValues: false,
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

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Identificator, CheckValues[ChangeType.Insert.ToString()].Item1.Identificator);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Surname, CheckValues[ChangeType.Insert.ToString()].Item1.Surname);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Identificator, CheckValues[ChangeType.Update.ToString()].Item1.Identificator);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, CheckValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Surname, CheckValues[ChangeType.Update.ToString()].Item1.Surname);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Identificator, CheckValues[ChangeType.Delete.ToString()].Item1.Identificator);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Surname, CheckValues[ChangeType.Delete.ToString()].Item1.Surname);

            Assert.IsTrue(AreAllDbObjectDisposed(naming));
            Assert.IsTrue(CountConversationEndpoints(naming) == 0);
        }

        [TestMethod]
        public void TestWithOldValues()
        {
            SqlTableDependency<WithDataAnnotationTestModel> tableDependency = null;
            string naming;

            Expression<Func<WithDataAnnotationTestModel, bool>> expression = p => p.Identificator == _id;
            ITableDependencyFilter filterExpression = new SqlTableDependencyFilter<WithDataAnnotationTestModel>(expression);

            try
            {
                tableDependency = new SqlTableDependency<WithDataAnnotationTestModel>(
                    ConnectionStringForSa,
                    includeOldValues: true,
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

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Identificator, CheckValues[ChangeType.Insert.ToString()].Item1.Identificator);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Surname, CheckValues[ChangeType.Insert.ToString()].Item1.Surname);
            Assert.IsNull(CheckValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Identificator, CheckValues[ChangeType.Update.ToString()].Item1.Identificator);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, CheckValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Surname, CheckValues[ChangeType.Update.ToString()].Item1.Surname);
            Assert.AreEqual(CheckValuesOld[ChangeType.Update.ToString()].Item2.Identificator, CheckValues[ChangeType.Insert.ToString()].Item2.Identificator);
            Assert.AreEqual(CheckValuesOld[ChangeType.Update.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item2.Name);
            Assert.AreEqual(CheckValuesOld[ChangeType.Update.ToString()].Item2.Surname, CheckValues[ChangeType.Insert.ToString()].Item2.Surname);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Identificator, CheckValues[ChangeType.Delete.ToString()].Item1.Identificator);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Surname, CheckValues[ChangeType.Delete.ToString()].Item1.Surname);
            Assert.IsNull(CheckValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(AreAllDbObjectDisposed(naming));
            Assert.IsTrue(CountConversationEndpoints(naming) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<WithDataAnnotationTestModel> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.Identificator = e.Entity.Identificator;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Surname = e.Entity.Surname;

                    if (e.EntityOldValues != null)
                    {
                        CheckValuesOld[ChangeType.Insert.ToString()].Item2.Identificator = e.EntityOldValues.Identificator;
                        CheckValuesOld[ChangeType.Insert.ToString()].Item2.Name = e.EntityOldValues.Name;
                        CheckValuesOld[ChangeType.Insert.ToString()].Item2.Surname = e.EntityOldValues.Surname;
                    }
                    else
                    {
                        CheckValuesOld[ChangeType.Insert.ToString()] = null;
                    }

                    break;

                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.Identificator = e.Entity.Identificator;
                    CheckValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Update.ToString()].Item2.Surname = e.Entity.Surname;

                    if (e.EntityOldValues != null)
                    {
                        CheckValuesOld[ChangeType.Update.ToString()].Item2.Identificator = e.EntityOldValues.Identificator;
                        CheckValuesOld[ChangeType.Update.ToString()].Item2.Name = e.EntityOldValues.Name;
                        CheckValuesOld[ChangeType.Update.ToString()].Item2.Surname = e.EntityOldValues.Surname;
                    }
                    else
                    {
                        CheckValuesOld[ChangeType.Update.ToString()] = null;
                    }

                    break;

                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.Identificator = e.Entity.Identificator;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Surname = e.Entity.Surname;

                    if (e.EntityOldValues != null)
                    {
                        CheckValuesOld[ChangeType.Delete.ToString()].Item2.Identificator = e.EntityOldValues.Identificator;
                        CheckValuesOld[ChangeType.Delete.ToString()].Item2.Name = e.EntityOldValues.Name;
                        CheckValuesOld[ChangeType.Delete.ToString()].Item2.Surname = e.EntityOldValues.Surname;
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
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandText = $"INSERT INTO [{SchemaName}].[{TableName}] ([Id], [Name], [Last Name]) VALUES (999, N'Iron', N'Man')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandText = $"INSERT INTO [{SchemaName}].[{TableName}] ([Id], [Name], [Last Name]) VALUES ({CheckValues[ChangeType.Insert.ToString()].Item1.Identificator}, N'{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', N'{CheckValues[ChangeType.Insert.ToString()].Item1.Surname}')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandText = $"UPDATE [{SchemaName}].[{TableName}] SET [Name] = N'Spider', [Last Name] = 'Man' WHERE [Id] = 999";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandText = $"UPDATE [{SchemaName}].[{TableName}] SET [Name] = N'{CheckValues[ChangeType.Update.ToString()].Item1.Name}', [Last Name] =  N'{CheckValues[ChangeType.Update.ToString()].Item1.Surname}' WHERE [Id] = {CheckValues[ChangeType.Update.ToString()].Item1.Identificator}";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandText = $"DELETE FROM [{SchemaName}].[{TableName}] WHERE [Id] = 999";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandText = $"DELETE FROM [{SchemaName}].[{TableName}] WHERE [Id] = {CheckValues[ChangeType.Delete.ToString()].Item1.Identificator}";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}