using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.ManagedDataAccess.Client;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Exceptions;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.OracleClient;

namespace TableDependency.IntegrationTest
{
    public class TriggerTypeTestOracleModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
        public DateTime Born { get; set; }
        public int Quantity { get; set; }
    }

    [TestClass]
    public class TriggerTypeTestOracle
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private const string TableName = "AAA_CheckTriggerType";
        private static int _counter;
        private static Dictionary<string, Tuple<TriggerTypeTestOracleModel, TriggerTypeTestOracleModel>> CheckValues = new Dictionary<string, Tuple<TriggerTypeTestOracleModel, TriggerTypeTestOracleModel>>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            OracleHelper.DropTable(ConnectionString, TableName);

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {TableName} (ID number(10), NAME varchar2(50), SURNAME varchar2(4000))";
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
            CheckValues = new Dictionary<string, Tuple<TriggerTypeTestOracleModel, TriggerTypeTestOracleModel>>();
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            OracleHelper.DropTable(ConnectionString, TableName);
        }

        [TestCategory("Oracle")]
        [TestMethod]
        [ExpectedException(typeof(DmlTriggerTypeException))]
        public void RaiseException1()
        {
            OracleTableDependency<TriggerTypeTestOracleModel> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<TriggerTypeTestOracleModel>(
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

        [TestCategory("Oracle")]
        [TestMethod]
        [ExpectedException(typeof(DmlTriggerTypeException))]
        public void RaiseException2()
        {
            OracleTableDependency<TriggerTypeTestOracleModel> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<TriggerTypeTestOracleModel>(
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

        [TestCategory("Oracle")]
        [TestMethod]
        [ExpectedException(typeof(DmlTriggerTypeException))]
        public void RaiseException3()
        {
            OracleTableDependency<TriggerTypeTestOracleModel> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<TriggerTypeTestOracleModel>(
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

        [TestCategory("Oracle")]
        [TestMethod]
        public void OnlyInsert()
        {
            OracleTableDependency<TriggerTypeTestOracleModel> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<TriggerTypeTestOracleModel>(
                    ConnectionString,
                    TableName,
                    null,
                    (IList<string>)null,
                    DmlTriggerType.Insert,
                    true);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(2000);

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
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestCategory("Oracle")]
        [TestMethod]
        public void OnlyDelete()
        {
            OracleTableDependency<TriggerTypeTestOracleModel> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<TriggerTypeTestOracleModel>(
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
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestCategory("Oracle")]
        [TestMethod]
        public void OnlyUpdate()
        {
            OracleTableDependency<TriggerTypeTestOracleModel> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<TriggerTypeTestOracleModel>(
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
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestCategory("Oracle")]
        [TestMethod]
        public void OnlyInsertDelete()
        {
            OracleTableDependency<TriggerTypeTestOracleModel> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<TriggerTypeTestOracleModel>(
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
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestCategory("Oracle")]
        [TestMethod]
        public void OnlyInsertUpdate()
        {
            OracleTableDependency<TriggerTypeTestOracleModel> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<TriggerTypeTestOracleModel>(
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
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestCategory("Oracle")]
        [TestMethod]
        public void DeleteInsertUpdate()
        {
            OracleTableDependency<TriggerTypeTestOracleModel> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<TriggerTypeTestOracleModel>(
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
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        [TestCategory("Oracle")]
        [TestMethod]
        public void All()
        {
            OracleTableDependency<TriggerTypeTestOracleModel> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<TriggerTypeTestOracleModel>(
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
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<TriggerTypeTestOracleModel> e)
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
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<TriggerTypeTestOracleModel, TriggerTypeTestOracleModel>(new TriggerTypeTestOracleModel { Id = 23, Name = "Pizza Mergherita", Surname = "Pizza Mergherita" }, new TriggerTypeTestOracleModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<TriggerTypeTestOracleModel, TriggerTypeTestOracleModel>(new TriggerTypeTestOracleModel { Id = 23, Name = "Pizza Funghi", Surname = "Pizza Mergherita" }, new TriggerTypeTestOracleModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<TriggerTypeTestOracleModel, TriggerTypeTestOracleModel>(new TriggerTypeTestOracleModel { Id = 23, Name = "Pizza Funghi", Surname = "Pizza Funghi" }, new TriggerTypeTestOracleModel()));

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"BEGIN INSERT INTO {TableName} (ID, NAME, SURNAME) VALUES ({CheckValues[ChangeType.Insert.ToString()].Item1.Id}, '{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', '{CheckValues[ChangeType.Insert.ToString()].Item1.Surname}'); END;";
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