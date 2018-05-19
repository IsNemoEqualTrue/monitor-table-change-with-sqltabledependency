using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient.BaseTests;

namespace TableDependency.SqlClient.IntegrationTests
{
    [TestClass]
    public class UpdateOfColumnTest : SqlTableDependencyBaseTest
    {
        private class EventForSpecificColumnsTestSqlServerModel
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public string Surname { get; set; }
            public DateTime Born { get; set; }
            public int Quantity { get; set; }
        }

        private static readonly string TableName = typeof(EventForSpecificColumnsTestSqlServerModel).Name;
        private static int _counter;
        private static Dictionary<string, Tuple<EventForSpecificColumnsTestSqlServerModel, EventForSpecificColumnsTestSqlServerModel>> _checkValues = new Dictionary<string, Tuple<EventForSpecificColumnsTestSqlServerModel, EventForSpecificColumnsTestSqlServerModel>>();
        private static Dictionary<string, Tuple<EventForSpecificColumnsTestSqlServerModel, EventForSpecificColumnsTestSqlServerModel>> _checkValuesOld = new Dictionary<string, Tuple<EventForSpecificColumnsTestSqlServerModel, EventForSpecificColumnsTestSqlServerModel>>();

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

                    sqlCommand.CommandText =
                        $"CREATE TABLE [{TableName}]( " +
                        "[Id][int] IDENTITY(1, 1) NOT NULL, " +
                        "[First Name] [nvarchar](50) NOT NULL, " +
                        "[Second Name] [nvarchar](50) NOT NULL, " +
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

            _checkValues.Clear();
            _checkValuesOld.Clear();

            _counter = 0;

            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<EventForSpecificColumnsTestSqlServerModel, EventForSpecificColumnsTestSqlServerModel>(new EventForSpecificColumnsTestSqlServerModel { Name = "Christian", Surname = "Del Bianco" }, new EventForSpecificColumnsTestSqlServerModel()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<EventForSpecificColumnsTestSqlServerModel, EventForSpecificColumnsTestSqlServerModel>(new EventForSpecificColumnsTestSqlServerModel { Name = "Velia" }, new EventForSpecificColumnsTestSqlServerModel()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<EventForSpecificColumnsTestSqlServerModel, EventForSpecificColumnsTestSqlServerModel>(new EventForSpecificColumnsTestSqlServerModel { Name = "Velia", Surname = "Del Bianco" }, new EventForSpecificColumnsTestSqlServerModel()));

            _checkValuesOld.Add(ChangeType.Insert.ToString(), new Tuple<EventForSpecificColumnsTestSqlServerModel, EventForSpecificColumnsTestSqlServerModel>(new EventForSpecificColumnsTestSqlServerModel { Name = "Christian", Surname = "Del Bianco" }, new EventForSpecificColumnsTestSqlServerModel()));
            _checkValuesOld.Add(ChangeType.Update.ToString(), new Tuple<EventForSpecificColumnsTestSqlServerModel, EventForSpecificColumnsTestSqlServerModel>(new EventForSpecificColumnsTestSqlServerModel { Name = "Velia" }, new EventForSpecificColumnsTestSqlServerModel()));
            _checkValuesOld.Add(ChangeType.Delete.ToString(), new Tuple<EventForSpecificColumnsTestSqlServerModel, EventForSpecificColumnsTestSqlServerModel>(new EventForSpecificColumnsTestSqlServerModel { Name = "Velia", Surname = "Del Bianco" }, new EventForSpecificColumnsTestSqlServerModel()));
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
        public void Test()
        {
            SqlTableDependency<EventForSpecificColumnsTestSqlServerModel> tableDependency = null;
            string naming;

            try
            {
                var mapper = new ModelToTableMapper<EventForSpecificColumnsTestSqlServerModel>();
                mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second Name");

                var updateOf = new UpdateOfModel<EventForSpecificColumnsTestSqlServerModel>();
                updateOf.Add(i => i.Surname);

                tableDependency = new SqlTableDependency<EventForSpecificColumnsTestSqlServerModel>(
                    ConnectionStringForTestUser, 
                    tableName: TableName, 
                    mapper: mapper, 
                    updateOf: updateOf);

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

            Assert.AreEqual(_counter, 2);

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Name, _checkValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Surname, _checkValues[ChangeType.Insert.ToString()].Item1.Surname);
            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Name, _checkValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Surname, _checkValues[ChangeType.Delete.ToString()].Item1.Surname);
            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming)== 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TestWithOldValues()
        {
            SqlTableDependency<EventForSpecificColumnsTestSqlServerModel> tableDependency = null;
            string naming;

            try
            {
                var mapper = new ModelToTableMapper<EventForSpecificColumnsTestSqlServerModel>();
                mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second Name");

                var updateOf = new UpdateOfModel<EventForSpecificColumnsTestSqlServerModel>();
                updateOf.Add(i => i.Surname);

                tableDependency = new SqlTableDependency<EventForSpecificColumnsTestSqlServerModel>(
                    ConnectionStringForTestUser,
                    includeOldValues: true,
                    tableName: TableName,
                    mapper: mapper,
                    updateOf: updateOf);

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

            Assert.AreEqual(_counter, 2);

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Name, _checkValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Surname, _checkValues[ChangeType.Insert.ToString()].Item1.Surname);
            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Name, _checkValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Surname, _checkValues[ChangeType.Delete.ToString()].Item1.Surname);
            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<EventForSpecificColumnsTestSqlServerModel> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    _checkValues[ChangeType.Insert.ToString()].Item2.Surname = e.Entity.Surname;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.Name = e.EntityOldValues.Name;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.Surname = e.EntityOldValues.Surname;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()] = null;
                    }

                    break;

                case ChangeType.Update:
                    Assert.Fail("Not expected event!");
                    break;

                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    _checkValues[ChangeType.Delete.ToString()].Item2.Surname = e.Entity.Surname;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.Name = e.EntityOldValues.Name;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.Surname = e.EntityOldValues.Surname;
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
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{_checkValues[ChangeType.Insert.ToString()].Item1.Name}', '{_checkValues[ChangeType.Insert.ToString()].Item1.Surname}')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = '{_checkValues[ChangeType.Update.ToString()].Item1.Name}'";
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