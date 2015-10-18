using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Exceptions;
using TableDependency.IntegrationTest.Helpers.SqlServer;
using TableDependency.IntegrationTest.Models;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    [TestClass]
    public class TriggerTypeTestSqlServer
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServerConnectionString"].ConnectionString;
        private const string TableName = "CheckTriggerType";
        private static int _counter;
        private static Dictionary<string, Tuple<Item, Item>> CheckValues = new Dictionary<string, Tuple<Item, Item>>();

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
                        "[Id] [int] IDENTITY(1, 1) NOT NULL, " +
                        "[Name] [NVARCHAR](50) NOT NULL, " +
                        "[Description] [NVARCHAR](50) NOT NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize()]
        public void TestInitialize()
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }

            _counter = 0;
            CheckValues = new Dictionary<string, Tuple<Item, Item>>();
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
        [ExpectedException(typeof(DmlTriggerTypeException))]
        public void RaiseException1()
        {
            SqlTableDependency<Item> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<Item>(
                    ConnectionString,
                    TableName,
                    null,
                    new List<string>() { "second name" }, 
                    DmlTriggerType.Insert, 
                    true);

                naming = tableDependency.DataBaseObjectsNamingConvention;
            }
            finally
            {
                tableDependency?.Dispose();
            }
           
            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestMethod]
        [ExpectedException(typeof(DmlTriggerTypeException))]
        public void RaiseException2()
        {
            SqlTableDependency<Item> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<Item>(
                    ConnectionString,
                    TableName,
                    null,
                    new List<string>() { "second name" },
                    DmlTriggerType.Delete,
                    true);

                naming = tableDependency.DataBaseObjectsNamingConvention;
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestMethod]
        [ExpectedException(typeof(DmlTriggerTypeException))]
        public void RaiseException3()
        {
            SqlTableDependency<Item> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<Item>(
                    ConnectionString,
                    TableName,
                    null,
                    new List<string>() { "second name" },
                    DmlTriggerType.Delete | DmlTriggerType.Insert,
                    true);

                naming = tableDependency.DataBaseObjectsNamingConvention;
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestMethod]
        public void OnlyInsert()
        {
            SqlTableDependency<Item> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<Item>(
                    ConnectionString,
                    TableName,
                    null,
                    (IList<string>)null,
                    DmlTriggerType.Insert,
                    true);
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

            Assert.AreEqual(_counter, 1);

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, "Pizza Mergherita");
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Description, "Pizza Mergherita");

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestMethod]
        public void OnlyDelete()
        {
            SqlTableDependency<Item> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<Item>(
                    ConnectionString,
                    TableName,
                    null,
                    (IList<string>)null,
                    DmlTriggerType.Delete,
                    true);
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

            Assert.AreEqual(_counter, 1);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Description, "Pizza Mergherita");

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestMethod]
        public void OnlyUdate()
        {
            SqlTableDependency<Item> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<Item>(
                    ConnectionString,
                    TableName,
                    null,
                    (IList<string>)null,
                    DmlTriggerType.Update,
                    true);
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

            Assert.AreEqual(_counter, 1);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Description, "Pizza Mergherita");

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestMethod]
        public void OnlyInsertDelete()
        {
            SqlTableDependency<Item> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<Item>(
                    ConnectionString,
                    TableName,
                    null,
                    (IList<string>)null,
                    DmlTriggerType.Insert | DmlTriggerType.Delete,
                    true);
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

            Assert.AreEqual(_counter, 2);

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, "Pizza Mergherita");
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Description, "Pizza Mergherita");

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Description, "Pizza Mergherita");

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestMethod]
        public void OnlyInsertUpdate()
        {
            SqlTableDependency<Item> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<Item>(
                    ConnectionString,
                    TableName,
                    null,
                    (IList<string>)null,
                    DmlTriggerType.Insert | DmlTriggerType.Update,
                    true);
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

            Assert.AreEqual(_counter, 2);

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, "Pizza Mergherita");
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Description, "Pizza Mergherita");

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Description, "Pizza Mergherita");

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestMethod]
        public void DeleteInsertUpdate()
        {
            SqlTableDependency<Item> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<Item>(
                    ConnectionString,
                    TableName,
                    null,
                    (IList<string>)null,
                    DmlTriggerType.Delete | DmlTriggerType.Insert | DmlTriggerType.Update,
                    true);
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

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, "Pizza Mergherita");
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Description, "Pizza Mergherita");

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Description, "Pizza Mergherita");

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Description, "Pizza Mergherita");

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestMethod]
        public void All()
        {
            SqlTableDependency<Item> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<Item>(
                    ConnectionString,
                    TableName,
                    null,
                    (IList<string>)null,
                    DmlTriggerType.All,
                    true);
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

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, "Pizza Mergherita");
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Description, "Pizza Mergherita");

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Description, "Pizza Mergherita");

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Description, "Pizza Mergherita");

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
                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Description = e.Entity.Description;
                    break;
                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Update.ToString()].Item2.Description = e.Entity.Description;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<Item, Item>(new Item { Id = 23, Name = "Pizza Mergherita", Description = "Pizza Mergherita" }, new Item()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<Item, Item>(new Item { Id = 23, Name = "Pizza Funghi", Description = "Pizza Mergherita" }, new Item()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<Item, Item>(new Item { Id = 23, Name = "Pizza Funghi", Description = "Pizza Funghi" }, new Item()));

            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name], [Description]) VALUES ('{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', '{CheckValues[ChangeType.Insert.ToString()].Item1.Description}')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = '{CheckValues[ChangeType.Update.ToString()].Item1.Name}'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);
                }
            }
        }
    }
}