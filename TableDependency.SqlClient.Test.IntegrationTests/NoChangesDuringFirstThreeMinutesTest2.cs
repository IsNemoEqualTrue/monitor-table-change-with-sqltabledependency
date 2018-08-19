using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient.Test.Base;

namespace TableDependency.SqlClient.Test.IntegrationTests
{
    [TestClass]
    public class NoChangesDuringFirstThreeMinutesTest2 : SqlTableDependencyBaseTest
    {
        private class NoChangesDuringFirstThreeMinutesTestSqlServer2Model
        {
            public int MenuId { get; set; }
            public string Name { get; set; }
            public string Surname { get; set; }
        }

        private static readonly string TableName = typeof(NoChangesDuringFirstThreeMinutesTestSqlServer2Model).Name;
        private static Dictionary<string, Tuple<NoChangesDuringFirstThreeMinutesTestSqlServer2Model, NoChangesDuringFirstThreeMinutesTestSqlServer2Model>> _checkValues1 = new Dictionary<string, Tuple<NoChangesDuringFirstThreeMinutesTestSqlServer2Model, NoChangesDuringFirstThreeMinutesTestSqlServer2Model>>();
        private static Dictionary<string, Tuple<NoChangesDuringFirstThreeMinutesTestSqlServer2Model, NoChangesDuringFirstThreeMinutesTestSqlServer2Model>> _checkValues2 = new Dictionary<string, Tuple<NoChangesDuringFirstThreeMinutesTestSqlServer2Model, NoChangesDuringFirstThreeMinutesTestSqlServer2Model>>();

        private static int _counter;

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

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([MenuId] [INT] NULL, [Name] [NVARCHAR](30) NULL, [Surname] [NVARCHAR](30) NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
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
            SqlTableDependency<NoChangesDuringFirstThreeMinutesTestSqlServer2Model> tableDependency = null;
            string dataBaseObjectsNamingConvention = null;

            try
            {
                tableDependency = new SqlTableDependency<NoChangesDuringFirstThreeMinutesTestSqlServer2Model>(ConnectionStringForTestUser, includeOldValues: true);
                tableDependency.OnChanged += TableDependency_Changed1;
                tableDependency.Start();
                dataBaseObjectsNamingConvention = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent1);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);

                Assert.AreEqual(_counter, 3);
                Assert.AreEqual(_checkValues1[ChangeType.Insert.ToString()].Item2.Name, _checkValues1[ChangeType.Insert.ToString()].Item1.Name);
                Assert.AreEqual(_checkValues1[ChangeType.Insert.ToString()].Item2.Surname, _checkValues1[ChangeType.Insert.ToString()].Item1.Surname);
                Assert.AreEqual(_checkValues1[ChangeType.Update.ToString()].Item2.Name, _checkValues1[ChangeType.Update.ToString()].Item1.Name);
                Assert.AreEqual(_checkValues1[ChangeType.Update.ToString()].Item2.Surname, _checkValues1[ChangeType.Update.ToString()].Item1.Surname);
                Assert.AreEqual(_checkValues1[ChangeType.Delete.ToString()].Item2.Name, _checkValues1[ChangeType.Delete.ToString()].Item1.Name);
                Assert.AreEqual(_checkValues1[ChangeType.Delete.ToString()].Item2.Surname, _checkValues1[ChangeType.Delete.ToString()].Item1.Surname);

                Thread.Sleep(7 * 10 * 1000);
                var f = new Task(ModifyTableContent2);
                f.Start();
                Thread.Sleep(1000 * 15 * 1);

                Assert.AreEqual(_counter, 6);
                Assert.AreEqual(_checkValues2[ChangeType.Insert.ToString()].Item2.Name, _checkValues2[ChangeType.Insert.ToString()].Item1.Name);
                Assert.AreEqual(_checkValues2[ChangeType.Insert.ToString()].Item2.Surname, _checkValues2[ChangeType.Insert.ToString()].Item1.Surname);
                Assert.AreEqual(_checkValues2[ChangeType.Update.ToString()].Item2.Name, _checkValues2[ChangeType.Update.ToString()].Item1.Name);
                Assert.AreEqual(_checkValues2[ChangeType.Update.ToString()].Item2.Surname, _checkValues2[ChangeType.Update.ToString()].Item1.Surname);
                Assert.AreEqual(_checkValues2[ChangeType.Delete.ToString()].Item2.Name, _checkValues2[ChangeType.Delete.ToString()].Item1.Name);
                Assert.AreEqual(_checkValues2[ChangeType.Delete.ToString()].Item2.Surname, _checkValues2[ChangeType.Delete.ToString()].Item1.Surname);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Thread.Sleep(5000);

            Assert.IsTrue(base.AreAllDbObjectDisposed(dataBaseObjectsNamingConvention));
            Assert.IsTrue(base.CountConversationEndpoints(dataBaseObjectsNamingConvention) == 0);
        }

        private static void TableDependency_Changed1(object sender, RecordChangedEventArgs<NoChangesDuringFirstThreeMinutesTestSqlServer2Model> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    if (e.Entity.MenuId == 1)
                    {
                        _checkValues1[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                        _checkValues1[ChangeType.Insert.ToString()].Item2.Surname = e.Entity.Surname;
                    }
                    else
                    {
                        _checkValues2[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                        _checkValues2[ChangeType.Insert.ToString()].Item2.Surname = e.Entity.Surname;
                    }

                    break;

                case ChangeType.Delete:
                    if (e.Entity.MenuId == 1)
                    {
                        _checkValues1[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                        _checkValues1[ChangeType.Delete.ToString()].Item2.Surname = e.Entity.Surname;
                    }
                    else
                    {
                        _checkValues2[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                        _checkValues2[ChangeType.Delete.ToString()].Item2.Surname = e.Entity.Surname;
                    }

                    break;

                case ChangeType.Update:
                    if (e.Entity.MenuId == 1)
                    {
                        _checkValues1[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                        _checkValues1[ChangeType.Update.ToString()].Item2.Surname = e.Entity.Surname;
                    }
                    else
                    {
                        _checkValues2[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                        _checkValues2[ChangeType.Update.ToString()].Item2.Surname = e.Entity.Surname;
                    }

                    break;
            }
        }

        private static void ModifyTableContent1()
        {
            _checkValues1.Add(ChangeType.Insert.ToString(), new Tuple<NoChangesDuringFirstThreeMinutesTestSqlServer2Model, NoChangesDuringFirstThreeMinutesTestSqlServer2Model>(new NoChangesDuringFirstThreeMinutesTestSqlServer2Model { MenuId = 1, Name = "Pizza Prosciutto", Surname = "Pizza Prosciutto" }, new NoChangesDuringFirstThreeMinutesTestSqlServer2Model()));
            _checkValues1.Add(ChangeType.Update.ToString(), new Tuple<NoChangesDuringFirstThreeMinutesTestSqlServer2Model, NoChangesDuringFirstThreeMinutesTestSqlServer2Model>(new NoChangesDuringFirstThreeMinutesTestSqlServer2Model { MenuId = 1, Name = "Pizza Napoletana", Surname = "Pizza Prosciutto" }, new NoChangesDuringFirstThreeMinutesTestSqlServer2Model()));
            _checkValues1.Add(ChangeType.Delete.ToString(), new Tuple<NoChangesDuringFirstThreeMinutesTestSqlServer2Model, NoChangesDuringFirstThreeMinutesTestSqlServer2Model>(new NoChangesDuringFirstThreeMinutesTestSqlServer2Model { MenuId = 1, Name = "Pizza Napoletana", Surname = "Pizza Prosciutto" }, new NoChangesDuringFirstThreeMinutesTestSqlServer2Model()));

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([MenuId], [Name], [Surname]) VALUES ({_checkValues1[ChangeType.Insert.ToString()].Item1.MenuId}, '{_checkValues1[ChangeType.Insert.ToString()].Item1.Name}', '{_checkValues1[ChangeType.Insert.ToString()].Item1.Surname}')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = '{_checkValues1[ChangeType.Update.ToString()].Item1.Name}' WHERE [MenuId] = " + _checkValues1[ChangeType.Update.ToString()].Item1.MenuId;
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = $"DELETE FROM [{TableName}] WHERE [MenuId] = " + _checkValues1[ChangeType.Delete.ToString()].Item1.MenuId;
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        private static void ModifyTableContent2()
        {
            _checkValues2.Add(ChangeType.Insert.ToString(), new Tuple<NoChangesDuringFirstThreeMinutesTestSqlServer2Model, NoChangesDuringFirstThreeMinutesTestSqlServer2Model>(new NoChangesDuringFirstThreeMinutesTestSqlServer2Model { MenuId = 2, Name = "Pizza Mergherita", Surname = "Pizza Mergherita" }, new NoChangesDuringFirstThreeMinutesTestSqlServer2Model()));
            _checkValues2.Add(ChangeType.Update.ToString(), new Tuple<NoChangesDuringFirstThreeMinutesTestSqlServer2Model, NoChangesDuringFirstThreeMinutesTestSqlServer2Model>(new NoChangesDuringFirstThreeMinutesTestSqlServer2Model { MenuId = 2, Name = "Pizza Funghi", Surname = "Pizza Mergherita" }, new NoChangesDuringFirstThreeMinutesTestSqlServer2Model()));
            _checkValues2.Add(ChangeType.Delete.ToString(), new Tuple<NoChangesDuringFirstThreeMinutesTestSqlServer2Model, NoChangesDuringFirstThreeMinutesTestSqlServer2Model>(new NoChangesDuringFirstThreeMinutesTestSqlServer2Model { MenuId = 2, Name = "Pizza Funghi", Surname = "Pizza Mergherita" }, new NoChangesDuringFirstThreeMinutesTestSqlServer2Model()));

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([MenuId], [Name], [Surname]) VALUES ({_checkValues2[ChangeType.Insert.ToString()].Item1.MenuId}, '{_checkValues2[ChangeType.Insert.ToString()].Item1.Name}', '{_checkValues2[ChangeType.Insert.ToString()].Item1.Surname}')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = '{_checkValues2[ChangeType.Update.ToString()].Item1.Name}' WHERE [MenuId] = " + _checkValues2[ChangeType.Update.ToString()].Item1.MenuId;
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}] WHERE [MenuId] = " + _checkValues2[ChangeType.Delete.ToString()].Item1.MenuId;
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}