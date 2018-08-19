using System;
using System.Collections.Generic;
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
    public class WithMapperTest : SqlTableDependencyBaseTest
    {
        private class WithMapperTestModel
        {
            public int Identificator { get; set; }
            public string Name { get; set; }
            public string Surname { get; set; }
        }

        // Cannot be static !!!
        private const int _id = 2;
        private static readonly string TableName = "WithMapperTestModelTable";
        private static int _counter;
        private static readonly Dictionary<string, Tuple<WithMapperTestModel, WithMapperTestModel>> CheckValues = new Dictionary<string, Tuple<WithMapperTestModel, WithMapperTestModel>>();
        private static readonly Dictionary<string, Tuple<WithMapperTestModel, WithMapperTestModel>> CheckValuesOld = new Dictionary<string, Tuple<WithMapperTestModel, WithMapperTestModel>>();

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID(N'{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText =
                        $"CREATE TABLE [{TableName}]( " +
                        "[Id] [int] NOT NULL, " +
                        "[Name] [nvarchar](50) NOT NULL, " +
                        "[Second Name] [nvarchar](50) NULL, " +
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
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }

            CheckValues.Clear();
            CheckValuesOld.Clear();

            _counter = 0;

            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<WithMapperTestModel, WithMapperTestModel>(new WithMapperTestModel { Identificator = _id, Surname = "Del Bianco", Name = "Christian" }, new WithMapperTestModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<WithMapperTestModel, WithMapperTestModel>(new WithMapperTestModel { Identificator = _id, Surname = "Nonna", Name = "Velia" }, new WithMapperTestModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<WithMapperTestModel, WithMapperTestModel>(new WithMapperTestModel { Identificator = _id, Surname = "Nonna", Name = "Velia" }, new WithMapperTestModel()));

            CheckValuesOld.Add(ChangeType.Insert.ToString(), new Tuple<WithMapperTestModel, WithMapperTestModel>(new WithMapperTestModel { Identificator = _id, Surname = "Del Bianco", Name = "Christian" }, new WithMapperTestModel()));
            CheckValuesOld.Add(ChangeType.Update.ToString(), new Tuple<WithMapperTestModel, WithMapperTestModel>(new WithMapperTestModel { Identificator = _id, Surname = "Nonna", Name = "Velia" }, new WithMapperTestModel()));
            CheckValuesOld.Add(ChangeType.Delete.ToString(), new Tuple<WithMapperTestModel, WithMapperTestModel>(new WithMapperTestModel { Identificator = _id, Surname = "Nonna", Name = "Velia" }, new WithMapperTestModel()));
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID(N'{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestMethod]
        public void Test()
        {
            SqlTableDependency<WithMapperTestModel> tableDependency = null;
            string naming;

            var mapper = new ModelToTableMapper<WithMapperTestModel>();
            mapper.AddMapping(c => c.Surname, "Second Name");
            mapper.AddMapping(c => c.Identificator, "Id");

            Expression<Func<WithMapperTestModel, bool>> expression = p => p.Identificator == _id;
            ITableDependencyFilter filterExpression = new SqlTableDependencyFilter<WithMapperTestModel>(expression, mapper);

            try
            {
                tableDependency = new SqlTableDependency<WithMapperTestModel>(
                    ConnectionStringForTestUser,
                    includeOldValues: false,
                    tableName: TableName,
                    mapper: mapper,
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
            SqlTableDependency<WithMapperTestModel> tableDependency = null;
            string naming;

            var mapper = new ModelToTableMapper<WithMapperTestModel>();
            mapper.AddMapping(c => c.Surname, "Second Name");
            mapper.AddMapping(c => c.Identificator, "Id");

            Expression<Func<WithMapperTestModel, bool>> expression = p => p.Identificator == _id;
            ITableDependencyFilter filterExpression = new SqlTableDependencyFilter<WithMapperTestModel>(expression, mapper);

            try
            {
                tableDependency = new SqlTableDependency<WithMapperTestModel>(
                    ConnectionStringForTestUser,
                    includeOldValues: true,
                    tableName: TableName,
                    mapper: mapper,
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

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<WithMapperTestModel> e)
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
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name], [Second Name]) VALUES (999, N'Iron', N'Man')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name], [Second Name]) VALUES ({CheckValues[ChangeType.Insert.ToString()].Item1.Identificator}, N'{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', N'{CheckValues[ChangeType.Insert.ToString()].Item1.Surname}')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = N'Spider', [Second Name] = 'Man' WHERE [Id] = 999";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = N'{CheckValues[ChangeType.Update.ToString()].Item1.Name}', [Second Name] =  N'{CheckValues[ChangeType.Update.ToString()].Item1.Surname}' WHERE [Id] = {CheckValues[ChangeType.Update.ToString()].Item1.Identificator}";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}] WHERE [Id] = 999";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}] WHERE [Id] = {CheckValues[ChangeType.Delete.ToString()].Item1.Identificator}";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}