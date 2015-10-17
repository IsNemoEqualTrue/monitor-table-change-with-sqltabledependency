using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.DataAccess.Client;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.IntegrationTest.Models;
using TableDependency.OracleClient;

namespace TableDependency.IntegrationTest
{
    [TestClass]
    public class NoMapperUseTestOracle
    {
        private static string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static int _counter;
        private static readonly Dictionary<string, Tuple<Item2, Item2>> CheckValues = new Dictionary<string, Tuple<Item2, Item2>>();
        private static readonly string TableName = typeof(Item2).Name.ToUpper();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            OracleHelper.DropTable(ConnectionString, TableName);
        }

        [TestInitialize()]
        public void TestInitialize()
        {
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

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            OracleHelper.DropTable(ConnectionString, TableName);
        }

        [TestMethod]
        public void EventForAllColumnsTest()
        {
            OracleTableDependency<Item2> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<Item2>(ConnectionString, TableName);
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
            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Item2> e)
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
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<Item2, Item2>(new Item2 { Name = "Christian", Surname = "Del Bianco" }, new Item2()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<Item2, Item2>(new Item2 { Name = "Velia", Surname = "Ceccarelli" }, new Item2()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<Item2, Item2>(new Item2 { Name = "Velia", Surname = "Ceccarelli" }, new Item2()));

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var sqlCommand = connection.CreateCommand())
                {
                    sqlCommand.CommandText = $"BEGIN INSERT INTO {TableName} (ID, NAME, SURNAME) VALUES (1, '{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', '{CheckValues[ChangeType.Insert.ToString()].Item1.Surname}'); END;";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"BEGIN UPDATE {TableName} SET NAME = '{CheckValues[ChangeType.Update.ToString()].Item1.Name}', SURNAME = '{CheckValues[ChangeType.Update.ToString()].Item1.Surname}'; END;";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"BEGIN DELETE FROM {TableName}; END;";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);
                }
            }
        }
    }
}