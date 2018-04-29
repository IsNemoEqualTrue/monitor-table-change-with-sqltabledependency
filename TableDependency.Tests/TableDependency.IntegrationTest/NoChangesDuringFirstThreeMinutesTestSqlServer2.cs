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
    public class NoChangesDuringFirstThreeMinutesTestSqlServer2Model
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
    }

    [TestClass]
    public class NoChangesDuringFirstThreeMinutesTestSqlServer2 : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = "NoChangesDuringFirstThreeMinutesTestSqlServer2Model";
        private static Dictionary<string, Tuple<NoChangesDuringFirstThreeMinutesTestSqlServer2Model, NoChangesDuringFirstThreeMinutesTestSqlServer2Model>> _checkValues = new Dictionary<string, Tuple<NoChangesDuringFirstThreeMinutesTestSqlServer2Model, NoChangesDuringFirstThreeMinutesTestSqlServer2Model>>();
        private static int _counter;
        private static string _mal = "First Time";

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

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [int] NOT NULL PRIMARY KEY, [Name] [NVARCHAR](50) NULL, [Surname] [NVARCHAR](MAX) NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
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
        public void AfterThreeMinutesICanGetNotifications()
        {            
            SqlTableDependency<NoChangesDuringFirstThreeMinutesTestSqlServer2Model> tableDependency = null;
            string dataBaseObjectsNamingConvention = null;

            try
            {
                tableDependency = new SqlTableDependency<NoChangesDuringFirstThreeMinutesTestSqlServer2Model>(ConnectionStringForTestUser);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                dataBaseObjectsNamingConvention = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(4 * 60 * 1000);
                Assert.IsTrue(base.AreAllDbObjectDisposed(dataBaseObjectsNamingConvention) == false);
                Assert.IsTrue(base.CountConversationEndpoints(dataBaseObjectsNamingConvention) != 0);

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 10 * 1);

                Assert.AreEqual(_counter, 3);
                Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Name, "Pizza Mergherita" + _mal);
                Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Surname, "Pizza Mergherita" + _mal);
                Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Name, "Pizza Funghi" + _mal);
                Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Surname, "Pizza Mergherita" + _mal);
                Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Name, "Pizza Funghi" + _mal);
                Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Surname, "Pizza Mergherita" + _mal);

                _mal = "Second round";
                
                Thread.Sleep(7 * 60 * 1000);
                var f = new Task(ModifyTableContent);
                f.Start();
                Thread.Sleep(1000 * 10 * 1);

                Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Name, "Pizza Mergherita" + _mal);
                Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Surname, "Pizza Mergherita" + _mal);
                Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Name, "Pizza Funghi" + _mal);
                Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Surname, "Pizza Mergherita" + _mal);
                Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Name, "Pizza Funghi" + _mal);
                Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Surname, "Pizza Mergherita" + _mal);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Thread.Sleep(5000);

            Assert.IsTrue(base.AreAllDbObjectDisposed(dataBaseObjectsNamingConvention));
            Assert.IsTrue(base.CountConversationEndpoints(dataBaseObjectsNamingConvention) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<NoChangesDuringFirstThreeMinutesTestSqlServer2Model> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _counter++;
                    _checkValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    _checkValues[ChangeType.Insert.ToString()].Item2.Surname = e.Entity.Surname;
                    break;

                case ChangeType.Delete:
                    _counter++;
                    _checkValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    _checkValues[ChangeType.Delete.ToString()].Item2.Surname = e.Entity.Surname;
                    break;

                case ChangeType.Update:
                    _counter++;
                    _checkValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                    _checkValues[ChangeType.Update.ToString()].Item2.Surname = e.Entity.Surname;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            _checkValues.Clear();
            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<NoChangesDuringFirstThreeMinutesTestSqlServer2Model, NoChangesDuringFirstThreeMinutesTestSqlServer2Model>(new NoChangesDuringFirstThreeMinutesTestSqlServer2Model { Id = 23, Name = "Pizza Mergherita" + _mal, Surname = "Pizza Mergherita" + _mal }, new NoChangesDuringFirstThreeMinutesTestSqlServer2Model()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<NoChangesDuringFirstThreeMinutesTestSqlServer2Model, NoChangesDuringFirstThreeMinutesTestSqlServer2Model>(new NoChangesDuringFirstThreeMinutesTestSqlServer2Model { Id = 23, Name = "Pizza Funghi" + _mal, Surname = "Pizza Mergherita" + _mal }, new NoChangesDuringFirstThreeMinutesTestSqlServer2Model()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<NoChangesDuringFirstThreeMinutesTestSqlServer2Model, NoChangesDuringFirstThreeMinutesTestSqlServer2Model>(new NoChangesDuringFirstThreeMinutesTestSqlServer2Model { Id = 23, Name = "Pizza Funghi" + _mal, Surname = "Pizza Funghi" + _mal }, new NoChangesDuringFirstThreeMinutesTestSqlServer2Model()));

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name], [Surname]) VALUES ({_checkValues[ChangeType.Insert.ToString()].Item1.Id}, '{_checkValues[ChangeType.Insert.ToString()].Item1.Name}', '{_checkValues[ChangeType.Insert.ToString()].Item1.Surname}')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = '{_checkValues[ChangeType.Update.ToString()].Item1.Name}'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}