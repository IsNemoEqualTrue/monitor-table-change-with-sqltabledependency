using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Base;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class EventForAllColumnsTest : Base.SqlTableDependencyBaseTest
    {
        private class EventForAllColumnsTestSqlServerModel
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public string Surname { get; set; }
            public DateTime Born { get; set; }
            public int Quantity { get; set; }
        }

        private static readonly string TableName = typeof(EventForAllColumnsTestSqlServerModel).Name;
        private static int _counter;
        private static Dictionary<string, Tuple<EventForAllColumnsTestSqlServerModel, EventForAllColumnsTestSqlServerModel>> _checkValues = new Dictionary<string, Tuple<EventForAllColumnsTestSqlServerModel, EventForAllColumnsTestSqlServerModel>>();
        private static Dictionary<string, Tuple<EventForAllColumnsTestSqlServerModel, EventForAllColumnsTestSqlServerModel>> _checkValuesOld = new Dictionary<string, Tuple<EventForAllColumnsTestSqlServerModel, EventForAllColumnsTestSqlServerModel>>();

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

            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<EventForAllColumnsTestSqlServerModel, EventForAllColumnsTestSqlServerModel>(new EventForAllColumnsTestSqlServerModel { Name = "Christian", Surname = "Del Bianco" }, new EventForAllColumnsTestSqlServerModel()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<EventForAllColumnsTestSqlServerModel, EventForAllColumnsTestSqlServerModel>(new EventForAllColumnsTestSqlServerModel { Name = "Velia", Surname = "Ceccarelli" }, new EventForAllColumnsTestSqlServerModel()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<EventForAllColumnsTestSqlServerModel, EventForAllColumnsTestSqlServerModel>(new EventForAllColumnsTestSqlServerModel { Name = "Velia", Surname = "Ceccarelli" }, new EventForAllColumnsTestSqlServerModel()));

            _checkValuesOld.Add(ChangeType.Insert.ToString(), new Tuple<EventForAllColumnsTestSqlServerModel, EventForAllColumnsTestSqlServerModel>(new EventForAllColumnsTestSqlServerModel { Name = "Christian", Surname = "Del Bianco" }, new EventForAllColumnsTestSqlServerModel()));
            _checkValuesOld.Add(ChangeType.Update.ToString(), new Tuple<EventForAllColumnsTestSqlServerModel, EventForAllColumnsTestSqlServerModel>(new EventForAllColumnsTestSqlServerModel { Name = "Velia", Surname = "Ceccarelli" }, new EventForAllColumnsTestSqlServerModel()));
            _checkValuesOld.Add(ChangeType.Delete.ToString(), new Tuple<EventForAllColumnsTestSqlServerModel, EventForAllColumnsTestSqlServerModel>(new EventForAllColumnsTestSqlServerModel { Name = "Velia", Surname = "Ceccarelli" }, new EventForAllColumnsTestSqlServerModel()));
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
            SqlTableDependency<EventForAllColumnsTestSqlServerModel> tableDependency = null;

            try
            {
                var mapper = new ModelToTableMapper<EventForAllColumnsTestSqlServerModel>();
                mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second Name");

                tableDependency = new SqlTableDependency<EventForAllColumnsTestSqlServerModel>(ConnectionStringForTestUser, tableName: TableName, mapper: mapper);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 3);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Name, _checkValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Surname, _checkValues[ChangeType.Insert.ToString()].Item1.Surname);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Name, _checkValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Surname, _checkValues[ChangeType.Update.ToString()].Item1.Surname);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Name, _checkValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Surname, _checkValues[ChangeType.Delete.ToString()].Item1.Surname);

            Assert.IsTrue(base.AreAllDbObjectDisposed(tableDependency.DataBaseObjectsNamingConvention));
            Assert.IsTrue(base.CountConversationEndpoints(tableDependency.DataBaseObjectsNamingConvention) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TestWithOldValue()
        {
            SqlTableDependency<EventForAllColumnsTestSqlServerModel> tableDependency = null;

            try
            {
                var mapper = new ModelToTableMapper<EventForAllColumnsTestSqlServerModel>();
                mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second Name");

                tableDependency = new SqlTableDependency<EventForAllColumnsTestSqlServerModel>(
                    ConnectionStringForTestUser, 
                    includeOldValues: true,
                    tableName: TableName, 
                    mapper: mapper);

                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Name, _checkValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Surname, _checkValues[ChangeType.Insert.ToString()].Item1.Surname);
            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Name, _checkValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Surname, _checkValues[ChangeType.Update.ToString()].Item1.Surname);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.Name, _checkValues[ChangeType.Insert.ToString()].Item2.Name);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.Surname, _checkValues[ChangeType.Insert.ToString()].Item2.Surname);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Name, _checkValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Surname, _checkValues[ChangeType.Delete.ToString()].Item1.Surname);
            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(tableDependency.DataBaseObjectsNamingConvention));
            Assert.IsTrue(base.CountConversationEndpoints(tableDependency.DataBaseObjectsNamingConvention) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<EventForAllColumnsTestSqlServerModel> e)
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
                    _checkValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                    _checkValues[ChangeType.Update.ToString()].Item2.Surname = e.Entity.Surname;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.Name = e.EntityOldValues.Name;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.Surname = e.EntityOldValues.Surname;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Update.ToString()] = null;
                    }

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
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = '{_checkValues[ChangeType.Update.ToString()].Item1.Name}', [Second Name] = '{_checkValues[ChangeType.Update.ToString()].Item1.Surname}'";
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