using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers;
using TableDependency.IntegrationTest.Helpers.SqlServer;
using TableDependency.IntegrationTest.Models;
using TableDependency.Mappers;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    [TestClass]
    public class UpdateOfUsingLambaTestSqlServer
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServerConnectionString"].ConnectionString;
        private static readonly string TableName = typeof(Item).Name.ToUpper();
        private static int _counter = 0;
        private static readonly Dictionary<string, Tuple<Item, Item>> CheckValues = new Dictionary<string, Tuple<Item, Item>>();

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

                    sqlCommand.CommandText =
                        $"CREATE TABLE [{TableName}]( " +
                        $"[Id][int] IDENTITY(1, 1) NOT NULL, " +
                        $"[Name] [NVARCHAR](50) NOT NULL, " +
                        $"[Description] [NVARCHAR](MAX) NOT NULL)";
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
            SqlTableDependency<Item> tableDependency = null;
            string naming = null;

            var updateOfModel = new UpdateOfModel<Item>();
            updateOfModel.Add(i => i.Description);

            try
            {
                tableDependency = new SqlTableDependency<Item>(ConnectionString, updateOfModel);
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
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Description, CheckValues[ChangeType.Insert.ToString()].Item1.Description);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, CheckValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Description, CheckValues[ChangeType.Update.ToString()].Item1.Description);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Description, CheckValues[ChangeType.Delete.ToString()].Item1.Description);

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Item> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Description = e.Entity.Description;
                    break;
                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Update.ToString()].Item2.Description = e.Entity.Description;
                    break;
                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Description = e.Entity.Description;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<Item, Item>(new Item { Name = "Christian", Description = "Del Bianco" }, new Item()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<Item, Item>(new Item { Name = "Velia", Description = "Ceccarelli" }, new Item()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<Item, Item>(new Item { Name = "Velia", Description = "Ceccarelli" }, new Item()));

            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name], [Description]) VALUES ('{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', '{CheckValues[ChangeType.Insert.ToString()].Item1.Description}')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = '{CheckValues[ChangeType.Update.ToString()].Item1.Name}'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Description] = '{CheckValues[ChangeType.Update.ToString()].Item1.Description}'";
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