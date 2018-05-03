using System;
using System.Collections.Generic;
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
    public class EventForAllColumnsTestSqlServerModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
        public DateTime Born { get; set; }
        public int Quantity { get; set; }
    }

    [TestClass]
    public class EventForAllColumnsTestSqlServer : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(EventForAllColumnsTestSqlServerModel).Name;
        private static int _counter;
        private static readonly Dictionary<string, Tuple<EventForAllColumnsTestSqlServerModel, EventForAllColumnsTestSqlServerModel>> CheckValues = new Dictionary<string, Tuple<EventForAllColumnsTestSqlServerModel, EventForAllColumnsTestSqlServerModel>>();

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
                        "[Id][int] IDENTITY(1, 1) NOT NULL, " +
                        "[First Name] [nvarchar](50) NOT NULL, " +
                        "[Second Name] [nvarchar](50) NOT NULL, " +
                        "[Born] [datetime] NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize()]
        public void TestInitialize()
        {
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
        public void EventForAllColumnsTest()
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
                Thread.Sleep(1000 * 5 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 3);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Surname, CheckValues[ChangeType.Insert.ToString()].Item1.Surname);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, CheckValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Surname, CheckValues[ChangeType.Update.ToString()].Item1.Surname);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Surname, CheckValues[ChangeType.Delete.ToString()].Item1.Surname);

            Assert.IsTrue(base.AreAllDbObjectDisposed(tableDependency.DataBaseObjectsNamingConvention));
            Assert.IsTrue(base.CountConversationEndpoints(tableDependency.DataBaseObjectsNamingConvention) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<EventForAllColumnsTestSqlServerModel> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Surname = e.Entity.Surname;
                    break;
                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Update.ToString()].Item2.Surname = e.Entity.Surname;
                    break;
                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Surname = e.Entity.Surname;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<EventForAllColumnsTestSqlServerModel, EventForAllColumnsTestSqlServerModel>(new EventForAllColumnsTestSqlServerModel { Name = "Christian", Surname = "Del Bianco" }, new EventForAllColumnsTestSqlServerModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<EventForAllColumnsTestSqlServerModel, EventForAllColumnsTestSqlServerModel>(new EventForAllColumnsTestSqlServerModel { Name = "Velia", Surname = "Ceccarelli" }, new EventForAllColumnsTestSqlServerModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<EventForAllColumnsTestSqlServerModel, EventForAllColumnsTestSqlServerModel>(new EventForAllColumnsTestSqlServerModel { Name = "Velia", Surname = "Ceccarelli" }, new EventForAllColumnsTestSqlServerModel()));

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', '{CheckValues[ChangeType.Insert.ToString()].Item1.Surname}')";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = '{CheckValues[ChangeType.Update.ToString()].Item1.Name}', [Second Name] = '{CheckValues[ChangeType.Update.ToString()].Item1.Surname}'";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}