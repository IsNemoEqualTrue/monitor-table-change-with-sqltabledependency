using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.DataAccess.Client;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.IntegrationTest.Models;
using TableDependency.OracleClient;

namespace TableDependency.IntegrationTest
{
    [TestClass]
    public class DataAnnotationTestOracle
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static readonly string TableName = Item4.GetTableName.ToUpper();
        private static int _counter = 0;
        private static readonly Dictionary<string, Tuple<Item4, Item4>> CheckValues = new Dictionary<string, Tuple<Item4, Item4>>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            OracleHelper.DropTable(ConnectionString, TableName);

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {TableName} (ID number(10), NAME varchar2(50), \"Long Description\" varchar2(4000))";
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
        public void Test()
        {
            OracleTableDependency<Item4> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<Item4>(ConnectionString);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(30000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Description, CheckValues[ChangeType.Insert.ToString()].Item1.Description);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Description, CheckValues[ChangeType.Delete.ToString()].Item1.Description);

            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Item4> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _counter++;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Id = e.Entity.Id;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Description = e.Entity.Description;
                    break;

                case ChangeType.Update:
                    _counter++;
                    CheckValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Update.ToString()].Item2.Description = e.Entity.Description;
                    break;

                case ChangeType.Delete:
                    _counter++;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Id = e.Entity.Id;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Description = e.Entity.Description;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<Item4, Item4>(new Item4 { Id = 23, Name = "Pizza Mergherita", Description = "Pizza Mergherita" }, new Item4()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<Item4, Item4>(new Item4 { Id = 23, Name = "Pizza Funghi", Description = "Pizza Funghi" }, new Item4()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<Item4, Item4>(new Item4 { Id = 23, Name = "Pizza Funghi", Description = "Pizza Funghi" }, new Item4()));

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"BEGIN INSERT INTO {TableName} (ID, NAME, \"{Item4.GetColumnName}\") VALUES ({CheckValues[ChangeType.Insert.ToString()].Item1.Id}, '{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', '{CheckValues[ChangeType.Insert.ToString()].Item1.Description}'); END;";
                    command.ExecuteNonQuery();
                    Thread.Sleep(2000);

                    command.CommandText = $"BEGIN UPDATE {TableName} SET NAME = '{CheckValues[ChangeType.Update.ToString()].Item1.Name}', \"{Item4.GetColumnName}\" = '{CheckValues[ChangeType.Update.ToString()].Item1.Description}'; END;";
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