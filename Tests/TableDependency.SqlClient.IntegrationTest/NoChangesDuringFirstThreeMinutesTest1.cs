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
    public class NoChangesDuringFirstThreeMinutesTest1 : SqlTableDependencyBaseTest
    {
        private class NoChangesDuringFirstThreeMinutesTestSqlServerModel
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public string Surname { get; set; }
        }

        private static readonly string TableName = typeof(NoChangesDuringFirstThreeMinutesTestSqlServerModel).Name;
        private static readonly Dictionary<string, Tuple<NoChangesDuringFirstThreeMinutesTestSqlServerModel, NoChangesDuringFirstThreeMinutesTestSqlServerModel>> CheckValues = new Dictionary<string, Tuple<NoChangesDuringFirstThreeMinutesTestSqlServerModel, NoChangesDuringFirstThreeMinutesTestSqlServerModel>>();
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

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [int] NOT NULL, [Name] [NVARCHAR](50) NULL, [Surname] [NVARCHAR](MAX) NULL)";
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
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<NoChangesDuringFirstThreeMinutesTestSqlServerModel, NoChangesDuringFirstThreeMinutesTestSqlServerModel>(new NoChangesDuringFirstThreeMinutesTestSqlServerModel { Id = 23, Name = "Pizza Mergherita", Surname = "Pizza Mergherita" }, new NoChangesDuringFirstThreeMinutesTestSqlServerModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<NoChangesDuringFirstThreeMinutesTestSqlServerModel, NoChangesDuringFirstThreeMinutesTestSqlServerModel>(new NoChangesDuringFirstThreeMinutesTestSqlServerModel { Id = 23, Name = "Pizza Funghi", Surname = "Pizza Mergherita" }, new NoChangesDuringFirstThreeMinutesTestSqlServerModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<NoChangesDuringFirstThreeMinutesTestSqlServerModel, NoChangesDuringFirstThreeMinutesTestSqlServerModel>(new NoChangesDuringFirstThreeMinutesTestSqlServerModel { Id = 23, Name = "Pizza Funghi", Surname = "Pizza Funghi" }, new NoChangesDuringFirstThreeMinutesTestSqlServerModel()));

            SqlTableDependency<NoChangesDuringFirstThreeMinutesTestSqlServerModel> tableDependency = null;
            string dataBaseObjectsNamingConvention;

            try
            {
                tableDependency = new SqlTableDependency<NoChangesDuringFirstThreeMinutesTestSqlServerModel>(ConnectionStringForTestUser);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                dataBaseObjectsNamingConvention = tableDependency.DataBaseObjectsNamingConvention;
                
                Thread.Sleep(4 * 60 * 1000);
                Assert.IsFalse(base.AreAllDbObjectDisposed(dataBaseObjectsNamingConvention));

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, "Pizza Mergherita");
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.IsTrue(base.AreAllDbObjectDisposed(dataBaseObjectsNamingConvention));
            Assert.IsTrue(base.CountConversationEndpoints(dataBaseObjectsNamingConvention) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<NoChangesDuringFirstThreeMinutesTestSqlServerModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _counter++;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Surname = e.Entity.Surname;
                    break;

                case ChangeType.Delete:
                    _counter++;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Surname = e.Entity.Surname;
                    break;

                case ChangeType.Update:
                    _counter++;
                    CheckValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Update.ToString()].Item2.Surname = e.Entity.Surname;
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
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name], [Surname]) VALUES ({CheckValues[ChangeType.Insert.ToString()].Item1.Id}, '{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', '{CheckValues[ChangeType.Insert.ToString()].Item1.Surname}')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = '{CheckValues[ChangeType.Update.ToString()].Item1.Name}'";
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