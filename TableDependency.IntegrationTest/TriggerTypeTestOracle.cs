using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.DataAccess.Client;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Exceptions;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.IntegrationTest.Models;
using TableDependency.OracleClient;

namespace TableDependency.IntegrationTest
{
    [TestClass]
    public class TriggerTypeTestOracle
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private const string TableName = "CheckTriggerType";
        private static int _counter;
        private static Dictionary<string, Tuple<Item, Item>> CheckValues = new Dictionary<string, Tuple<Item, Item>>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            OracleHelper.DropTable(ConnectionString, TableName);

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {TableName} (ID number(10), NAME varchar2(50), DESCRIPTION varchar2(4000))";
                    command.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize()]
        public void TestInitialize()
        {
            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"DELETE FROM {TableName}";
                    command.ExecuteNonQuery();
                }
            }

            _counter = 0;
            CheckValues = new Dictionary<string, Tuple<Item, Item>>();
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            OracleHelper.DropTable(ConnectionString, TableName);
        }

        [TestMethod]
        [ExpectedException(typeof(DmlTriggerTypeException))]
        public void RaiseException1()
        {
            OracleTableDependency<Item> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<Item>(
                    ConnectionString,
                    TableName,
                    null,
                    new List<string>() { "NAME" },
                    DmlTriggerType.Insert,
                    true);

                naming = tableDependency.DataBaseObjectsNamingConvention;
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestMethod]
        [ExpectedException(typeof(DmlTriggerTypeException))]
        public void RaiseException2()
        {
            OracleTableDependency<Item> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<Item>(
                    ConnectionString,
                    TableName,
                    null,
                    new List<string>() { "NAME" },
                    DmlTriggerType.Delete,
                    true);

                naming = tableDependency.DataBaseObjectsNamingConvention;
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestMethod]
        [ExpectedException(typeof(DmlTriggerTypeException))]
        public void RaiseException3()
        {
            OracleTableDependency<Item> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<Item>(
                    ConnectionString,
                    TableName,
                    null,
                    new List<string>() { "NAME" },
                    DmlTriggerType.Delete | DmlTriggerType.Insert,
                    true);

                naming = tableDependency.DataBaseObjectsNamingConvention;
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestMethod]
        public void OnlyInsert()
        {
            OracleTableDependency<Item> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<Item>(
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

            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestMethod]
        public void OnlyDelete()
        {
            OracleTableDependency<Item> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<Item>(
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

            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestMethod]
        public void OnlyUpdate()
        {
            OracleTableDependency<Item> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<Item>(
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

            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestMethod]
        public void OnlyInsertDelete()
        {
            OracleTableDependency<Item> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<Item>(
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

            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestMethod]
        public void OnlyInsertUpdate()
        {
            OracleTableDependency<Item> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<Item>(
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

            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestMethod]
        public void DeleteInsertUpdate()
        {
            OracleTableDependency<Item> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<Item>(
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

            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestMethod]
        public void All()
        {
            OracleTableDependency<Item> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<Item>(
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

            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, naming));
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
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<Item, Item>(new Item { Id = 23, Name = "Pizza Mergherita", Description = "Pizza Mergherita" }, new Item()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<Item, Item>(new Item { Id = 23, Name = "Pizza Funghi", Description = "Pizza Mergherita" }, new Item()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<Item, Item>(new Item { Id = 23, Name = "Pizza Funghi", Description = "Pizza Funghi" }, new Item()));

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"BEGIN INSERT INTO {TableName} (ID, NAME, DESCRIPTION) VALUES ({CheckValues[ChangeType.Insert.ToString()].Item1.Id}, '{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', '{CheckValues[ChangeType.Insert.ToString()].Item1.Description}'); END;";
                    command.ExecuteNonQuery();
                    Thread.Sleep(2000);

                    command.CommandText = $"BEGIN UPDATE {TableName} SET NAME = '{CheckValues[ChangeType.Update.ToString()].Item1.Name}'; END;";
                    command.ExecuteNonQuery();
                    Thread.Sleep(2000);

                    command.CommandText = $"BEGIN DELETE FROM {TableName}; END;";
                    command.ExecuteNonQuery();
                    Thread.Sleep(2000);
                }
            }
        }
    }
}