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
using TableDependency.Mappers;
using TableDependency.OracleClient;

namespace TableDependency.IntegrationTest
{
    public class EventForAllColumnsTestOracleModel
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public int Qty { get; set; }
    }

    [TestClass]
    public class EventForAllColumnsTestOracle
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static readonly string TableName = "AAAA_Table".ToUpper();
        private static int _counter = 0;
        private static readonly Dictionary<string, Tuple<EventForAllColumnsTestOracleModel, EventForAllColumnsTestOracleModel>> CheckValues = new Dictionary<string, Tuple<EventForAllColumnsTestOracleModel, EventForAllColumnsTestOracleModel>>();

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

        [TestInitialize()]
        public void TestInitialize()
        {
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            OracleHelper.DropTable(ConnectionString, TableName);
        }

        [TestMethod]
        public void Test()
        {
            OracleTableDependency<EventForAllColumnsTestOracleModel> tableDependency = null;
            string naming = null;

            try
            {
                var mapper = new ModelToTableMapper<EventForAllColumnsTestOracleModel>();
                mapper.AddMapping(c => c.Description, "Long Description");

                tableDependency = new OracleTableDependency<EventForAllColumnsTestOracleModel>(ConnectionString, TableName, mapper);
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

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<EventForAllColumnsTestOracleModel> e)
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
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<EventForAllColumnsTestOracleModel, EventForAllColumnsTestOracleModel>(new EventForAllColumnsTestOracleModel { Id = 23, Name = "Pizza Mergherita", Description = "Pizza Mergherita" }, new EventForAllColumnsTestOracleModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<EventForAllColumnsTestOracleModel, EventForAllColumnsTestOracleModel>(new EventForAllColumnsTestOracleModel { Id = 23, Name = "Pizza Funghi", Description = "Pizza Funghi" }, new EventForAllColumnsTestOracleModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<EventForAllColumnsTestOracleModel, EventForAllColumnsTestOracleModel>(new EventForAllColumnsTestOracleModel { Id = 23, Name = "Pizza Funghi", Description = "Pizza Funghi" }, new EventForAllColumnsTestOracleModel()));

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"BEGIN INSERT INTO {TableName} (ID, NAME, \"Long Description\") VALUES ({CheckValues[ChangeType.Insert.ToString()].Item1.Id}, '{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', '{CheckValues[ChangeType.Insert.ToString()].Item1.Description}'); END;";
                    command.ExecuteNonQuery();
                    Thread.Sleep(2000);

                    command.CommandText = $"BEGIN UPDATE {TableName} SET NAME = '{CheckValues[ChangeType.Update.ToString()].Item1.Name}', \"Long Description\" = '{CheckValues[ChangeType.Update.ToString()].Item1.Description}'; END;";
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