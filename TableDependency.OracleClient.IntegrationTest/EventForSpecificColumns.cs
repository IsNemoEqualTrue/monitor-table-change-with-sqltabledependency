using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.DataAccess.Client;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.OracleClient.IntegrationTest.Helpers;
using TableDependency.OracleClient.IntegrationTest.Model;

namespace TableDependency.OracleClient.IntegrationTest
{
    [TestClass]
    public class EventForSpecificColumns
    {
        private static int _counter = 0;
        private static readonly Dictionary<string, Tuple<Item, Item>> CheckValues = new Dictionary<string, Tuple<Item, Item>>();

        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private static readonly string TableName = "AAAA_Table".ToUpper();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            Helper.DropTable(ConnectionString, TableName);
        }

        [TestInitialize()]
        public void TestInitialize()
        {
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
            Helper.DropTable(ConnectionString, TableName);
        }

        [TestMethod]
        public void EventForSpecificColumnsTest()
        {
            OracleTableDependency<Item> tableDependency = null;
            string naming = null;

            try
            {
                var mapper = new ModelToTableMapper<Item>();
                mapper.AddMapping(c => c.Description, "Long Description");

                tableDependency = new OracleTableDependency<Item>(ConnectionString, TableName, mapper, new List<string>() { "NAME" });
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
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Description, CheckValues[ChangeType.Insert.ToString()].Item1.Description);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Description, CheckValues[ChangeType.Delete.ToString()].Item1.Description);
            Assert.IsTrue(Helper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Item> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _counter++;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Description = e.Entity.Description;
                    break;
                case ChangeType.Delete:
                    _counter++;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Description = e.Entity.Description;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<Item, Item>(new Item { Name = "Pizza Mergherita", Description = "Pizza Mergherita" }, new Item()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<Item, Item>(new Item { Name = "Pizza Mergherita", Description = "Pizza Funghi" }, new Item()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<Item, Item>(new Item { Name = "Pizza Mergherita", Description = "Pizza Funghi" }, new Item()));

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var sqlCommand = connection.CreateCommand())
                {
                    sqlCommand.CommandText = 
                        $"BEGIN INSERT INTO {TableName} (ID, NAME, \"Long Description\") VALUES (100, '{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', '{CheckValues[ChangeType.Insert.ToString()].Item1.Description}'); " +
                        $"UPDATE {TableName} SET \"Long Description\" = '{CheckValues[ChangeType.Update.ToString()].Item1.Description}'; " +
                        $"DELETE FROM {TableName}; END;";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(2000);
                }
            }
        }
    }
}