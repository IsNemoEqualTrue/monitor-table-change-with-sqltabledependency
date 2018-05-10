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
    public class TableNameFromModelClassNameAndUpdateOfTestSqlServerModel
    {
        public long Id { get; set; }

        public string Name { get; set; }

        [Column(ColumnName)]
        public string FamilyName { get; set; }

        private const string ColumnName = "SURNAME";

        public static string GetColumnName => ColumnName;
    }

    [TestClass]
    public class TableNameFromModelClassNameAndUpdateOfTest : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(TableNameFromModelClassNameAndUpdateOfTestSqlServerModel).Name.ToUpper();
        private static Dictionary<string, Tuple<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel, TableNameFromModelClassNameAndUpdateOfTestSqlServerModel>> _checkValues = new Dictionary<string, Tuple<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel, TableNameFromModelClassNameAndUpdateOfTestSqlServerModel>>();
        private static Dictionary<string, Tuple<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel, TableNameFromModelClassNameAndUpdateOfTestSqlServerModel>> _checkValuesOld = new Dictionary<string, Tuple<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel, TableNameFromModelClassNameAndUpdateOfTestSqlServerModel>>();
        private static int _counter;

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText =
                        $"CREATE TABLE [{TableName}]( " +
                        "[Id] [int] IDENTITY(1, 1) NOT NULL, " +
                        "[Name] [NVARCHAR](50) NULL, " +
                        "[Surname] [NVARCHAR](MAX) NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize()]
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

            _checkValues.Clear();
            _checkValuesOld.Clear();

            _counter = 0;

            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel, TableNameFromModelClassNameAndUpdateOfTestSqlServerModel>(new TableNameFromModelClassNameAndUpdateOfTestSqlServerModel { Id = 23, Name = "Pizza Mergherita", FamilyName = "Pizza Mergherita" }, new TableNameFromModelClassNameAndUpdateOfTestSqlServerModel()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel, TableNameFromModelClassNameAndUpdateOfTestSqlServerModel>(new TableNameFromModelClassNameAndUpdateOfTestSqlServerModel { Id = 23, Name = "Pizza Funghi", FamilyName = "Pizza Mergherita" }, new TableNameFromModelClassNameAndUpdateOfTestSqlServerModel()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel, TableNameFromModelClassNameAndUpdateOfTestSqlServerModel>(new TableNameFromModelClassNameAndUpdateOfTestSqlServerModel { Id = 23, Name = "Pizza Funghi", FamilyName = "Pizza Mergherita" }, new TableNameFromModelClassNameAndUpdateOfTestSqlServerModel()));

            _checkValuesOld.Add(ChangeType.Insert.ToString(), new Tuple<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel, TableNameFromModelClassNameAndUpdateOfTestSqlServerModel>(new TableNameFromModelClassNameAndUpdateOfTestSqlServerModel { Id = 23, Name = "Pizza Mergherita", FamilyName = "Pizza Mergherita" }, new TableNameFromModelClassNameAndUpdateOfTestSqlServerModel()));
            _checkValuesOld.Add(ChangeType.Update.ToString(), new Tuple<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel, TableNameFromModelClassNameAndUpdateOfTestSqlServerModel>(new TableNameFromModelClassNameAndUpdateOfTestSqlServerModel { Id = 23, Name = "Pizza Funghi", FamilyName = "Pizza Mergherita" }, new TableNameFromModelClassNameAndUpdateOfTestSqlServerModel()));
            _checkValuesOld.Add(ChangeType.Delete.ToString(), new Tuple<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel, TableNameFromModelClassNameAndUpdateOfTestSqlServerModel>(new TableNameFromModelClassNameAndUpdateOfTestSqlServerModel { Id = 23, Name = "Pizza Funghi", FamilyName = "Pizza Mergherita" }, new TableNameFromModelClassNameAndUpdateOfTestSqlServerModel()));
        }

        [ClassCleanup()]
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
        public void Test()
        {
            SqlTableDependency<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel> tableDependency = null;
            string naming;

            try
            {
                UpdateOfModel<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel> updateOf = new UpdateOfModel<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel>();
                updateOf.Add(model => model.FamilyName);

                tableDependency = new SqlTableDependency<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel>(ConnectionStringForTestUser, updateOf: updateOf);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 5 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 2);

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Name, _checkValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.FamilyName, _checkValues[ChangeType.Insert.ToString()].Item1.FamilyName);
            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Name, _checkValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.FamilyName, _checkValues[ChangeType.Delete.ToString()].Item1.FamilyName);
            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TestWithOldValues()
        {
            SqlTableDependency<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel> tableDependency = null;
            string naming;

            try
            {
                UpdateOfModel<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel> updateOf = new UpdateOfModel<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel>();
                updateOf.Add(model => model.FamilyName);

                tableDependency = new SqlTableDependency<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel>(ConnectionStringForTestUser, includeOldValues: true, updateOf: updateOf);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 5 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 2);

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Name, _checkValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.FamilyName, _checkValues[ChangeType.Insert.ToString()].Item1.FamilyName);
            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Name, _checkValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.FamilyName, _checkValues[ChangeType.Delete.ToString()].Item1.FamilyName);
            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.Id = e.Entity.Id;
                    _checkValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    _checkValues[ChangeType.Insert.ToString()].Item2.FamilyName = e.Entity.FamilyName;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.Id = e.EntityOldValues.Id;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.Name = e.EntityOldValues.Name;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.FamilyName = e.EntityOldValues.FamilyName;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()] = null;
                    }

                    break;

                case ChangeType.Update:
                    Assert.Fail("No Update event expected!");
                    break;

                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.Id = e.Entity.Id;
                    _checkValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    _checkValues[ChangeType.Delete.ToString()].Item2.FamilyName = e.Entity.FamilyName;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.Id = e.EntityOldValues.Id;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.Name = e.EntityOldValues.Name;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.FamilyName = e.EntityOldValues.FamilyName;
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
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name], [SURNAME]) VALUES ('{_checkValues[ChangeType.Insert.ToString()].Item1.Name}', '{_checkValues[ChangeType.Insert.ToString()].Item1.FamilyName}')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = '{_checkValues[ChangeType.Update.ToString()].Item1.Name}'";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}