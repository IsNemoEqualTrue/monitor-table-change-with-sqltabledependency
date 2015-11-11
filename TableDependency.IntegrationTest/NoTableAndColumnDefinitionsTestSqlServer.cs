using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.SqlServer;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    public class NoTableAndColumnDefinitionsTestSqlServerModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
        public DateTime Born { get; set; }
        public int Quantity { get; set; }
    }

    [TestClass]
    public class NoTableAndColumnDefinitionsTestSqlServerTest
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServerConnectionString"].ConnectionString;
        private const string TableName = "NoTableAndColumnDefinitionsTestSqlServerModel";
        private static int _counter;
        private static readonly Dictionary<string, Tuple<NoTableAndColumnDefinitionsTestSqlServerModel, NoTableAndColumnDefinitionsTestSqlServerModel>> CheckValues = new Dictionary<string, Tuple<NoTableAndColumnDefinitionsTestSqlServerModel, NoTableAndColumnDefinitionsTestSqlServerModel>>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id][int] IDENTITY(1, 1) NOT NULL, [Name] [NVARCHAR](50) NOT NULL, [Surname] [NVARCHAR](50) NOT NULL)";
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
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestMethod]
        public void EventForAllColumnsTest()
        {
    SqlTableDependency<NoTableAndColumnDefinitionsTestSqlServerModel> tableDependency = null;
    string naming = null;

    try
    {
        tableDependency = new SqlTableDependency<NoTableAndColumnDefinitionsTestSqlServerModel>(ConnectionString);
        tableDependency.OnChanged += TableDependency_Changed;
        tableDependency.Start();
        naming = tableDependency.DataBaseObjectsNamingConvention;

        Thread.Sleep(5000);

        var t = new Task(ModifyTableContent);
        t.Start();
        t.Wait(20000);
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
            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<NoTableAndColumnDefinitionsTestSqlServerModel> e)
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
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<NoTableAndColumnDefinitionsTestSqlServerModel, NoTableAndColumnDefinitionsTestSqlServerModel>(new NoTableAndColumnDefinitionsTestSqlServerModel { Name = "Christian", Surname = "Del Bianco" }, new NoTableAndColumnDefinitionsTestSqlServerModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<NoTableAndColumnDefinitionsTestSqlServerModel, NoTableAndColumnDefinitionsTestSqlServerModel>(new NoTableAndColumnDefinitionsTestSqlServerModel { Name = "Velia", Surname = "Ceccarelli" }, new NoTableAndColumnDefinitionsTestSqlServerModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<NoTableAndColumnDefinitionsTestSqlServerModel, NoTableAndColumnDefinitionsTestSqlServerModel>(new NoTableAndColumnDefinitionsTestSqlServerModel { Name = "Velia", Surname = "Ceccarelli" }, new NoTableAndColumnDefinitionsTestSqlServerModel()));

            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name], [Surname]) VALUES ('{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', '{CheckValues[ChangeType.Insert.ToString()].Item1.Surname}')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = '{CheckValues[ChangeType.Update.ToString()].Item1.Name}', [Surname] = '{CheckValues[ChangeType.Update.ToString()].Item1.Surname}'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);
                }
            }
        }
    }
}