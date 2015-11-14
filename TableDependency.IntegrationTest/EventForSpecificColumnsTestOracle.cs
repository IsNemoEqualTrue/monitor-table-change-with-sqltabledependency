using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.Mappers;
using TableDependency.OracleClient;
using Oracle.ManagedDataAccess.Client;

namespace TableDependency.IntegrationTest
{
    public class EventForSpecificColumnsTestOracleModel
    {
        // *****************************************************
        // Generic tests
        // *****************************************************
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
        public DateTime Born { get; set; }
        public int Quantity { get; set; }
    }

    [TestClass]
    public class EventForSpecificColumnsTestOracle
    {
        private static int _counter = 0;
        private static readonly Dictionary<string, Tuple<EventForSpecificColumnsTestOracleModel, EventForSpecificColumnsTestOracleModel>> CheckValues = new Dictionary<string, Tuple<EventForSpecificColumnsTestOracleModel, EventForSpecificColumnsTestOracleModel>>();

        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static readonly string TableName = "AAAA_Table".ToUpper();

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

        [TestCategory("Oracle")]
        [TestMethod]
        public void EventForSpecificColumnsTest()
        {
            OracleTableDependency<EventForSpecificColumnsTestOracleModel> tableDependency = null;
            string naming = null;

            try
            {
                var mapper = new ModelToTableMapper<EventForSpecificColumnsTestOracleModel>();
                mapper.AddMapping(c => c.Surname, "Long Description");

                tableDependency = new OracleTableDependency<EventForSpecificColumnsTestOracleModel>(ConnectionString, TableName, mapper, new List<string>() { "NAME" });
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
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Surname, CheckValues[ChangeType.Insert.ToString()].Item1.Surname);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Surname, CheckValues[ChangeType.Delete.ToString()].Item1.Surname);
            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<EventForSpecificColumnsTestOracleModel> e)
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
            }
        }

        private static void ModifyTableContent()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<EventForSpecificColumnsTestOracleModel, EventForSpecificColumnsTestOracleModel>(new EventForSpecificColumnsTestOracleModel { Name = "Pizza Mergherita", Surname = "Pizza Mergherita" }, new EventForSpecificColumnsTestOracleModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<EventForSpecificColumnsTestOracleModel, EventForSpecificColumnsTestOracleModel>(new EventForSpecificColumnsTestOracleModel { Name = "Pizza Mergherita", Surname = "Pizza Funghi" }, new EventForSpecificColumnsTestOracleModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<EventForSpecificColumnsTestOracleModel, EventForSpecificColumnsTestOracleModel>(new EventForSpecificColumnsTestOracleModel { Name = "Pizza Mergherita", Surname = "Pizza Funghi" }, new EventForSpecificColumnsTestOracleModel()));

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var sqlCommand = connection.CreateCommand())
                {
                    sqlCommand.CommandText = 
                        $"BEGIN INSERT INTO {TableName} (ID, NAME, \"Long Description\") VALUES (100, '{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', '{CheckValues[ChangeType.Insert.ToString()].Item1.Surname}'); " +
                        $"UPDATE {TableName} SET \"Long Description\" = '{CheckValues[ChangeType.Update.ToString()].Item1.Surname}'; " +
                        $"DELETE FROM {TableName}; END;";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(2000);
                }
            }
        }
    }
}