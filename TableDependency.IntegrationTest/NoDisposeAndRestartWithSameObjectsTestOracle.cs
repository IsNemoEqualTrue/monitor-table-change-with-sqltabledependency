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
    public class TestOracleModel
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public int Qty { get; set; }
    }

    [TestClass]
    public class NoDisposeAndRestartWithSameObjectsTestOracle
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static string TableName = "aaesdel".ToUpper();
        private static int _counter;
        private static Dictionary<string, Tuple<TestOracleModel, TestOracleModel>> _checkValues = new Dictionary<string, Tuple<TestOracleModel, TestOracleModel>>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            OracleHelper.DropTable(ConnectionString, TableName);

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {TableName} (ID NUMBER(10), NAME VARCHAR2(50), \"Long Description\" VARCHAR2(4000))";
                    command.ExecuteNonQuery();
                }
            }
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            OracleHelper.DropTable(ConnectionString, TableName);
        }

        private void RunFirstTime(string namingToUse)
        {
            var mapper = new ModelToTableMapper<TestOracleModel>();
            mapper.AddMapping(c => c.Description, "Long Description").AddMapping(c => c.Name, "Name");

            var tableDependency = new OracleTableDependency<TestOracleModel>(ConnectionString, TableName, mapper, false, namingToUse);
            tableDependency.OnChanged += TableDependency_Changed;
            tableDependency.Start(60, 120);
        }

        [TestMethod]
        public void Test()
        {
            var namingToUse = "AAAOSTREGA";

            var mapper = new ModelToTableMapper<TestOracleModel>();
            mapper.AddMapping(c => c.Description, "Long Description").AddMapping(c => c.Name, "Name");

            RunFirstTime(namingToUse);
            Thread.Sleep(3 * 60 * 1000);

            using (var tableDependency = new OracleTableDependency<TestOracleModel>(ConnectionString, TableName, mapper, true, namingToUse))
            {
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                Assert.AreEqual(tableDependency.DataBaseObjectsNamingConvention, namingToUse);

                Thread.Sleep(1 * 25 * 1000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(1 * 60 * 1000);
            }

            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, namingToUse));
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Name, _checkValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Description, _checkValues[ChangeType.Insert.ToString()].Item1.Description);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Name, _checkValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Description, _checkValues[ChangeType.Update.ToString()].Item1.Description);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Name, _checkValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Description, _checkValues[ChangeType.Delete.ToString()].Item1.Description);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<TestOracleModel> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    _checkValues[ChangeType.Insert.ToString()].Item2.Description = e.Entity.Description;
                    break;
                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                    _checkValues[ChangeType.Update.ToString()].Item2.Description = e.Entity.Description;
                    break;
                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    _checkValues[ChangeType.Delete.ToString()].Item2.Description = e.Entity.Description;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<TestOracleModel, TestOracleModel>(new TestOracleModel { Name = "Christian", Description = "Del Bianco" }, new TestOracleModel()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<TestOracleModel, TestOracleModel>(new TestOracleModel { Name = "Velia", Description = "Ceccarelli" }, new TestOracleModel()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<TestOracleModel, TestOracleModel>(new TestOracleModel { Name = "Velia", Description = "Ceccarelli" }, new TestOracleModel()));

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"BEGIN INSERT INTO {TableName} (ID, NAME, \"Long Description\") VALUES ({_checkValues[ChangeType.Insert.ToString()].Item1.Id}, '{_checkValues[ChangeType.Insert.ToString()].Item1.Name}', '{_checkValues[ChangeType.Insert.ToString()].Item1.Description}'); END;";
                    command.ExecuteNonQuery();
                    Thread.Sleep(500);

                    command.CommandText = $"BEGIN UPDATE {TableName} SET NAME = '{_checkValues[ChangeType.Update.ToString()].Item1.Name}', \"Long Description\" = '{_checkValues[ChangeType.Update.ToString()].Item1.Description}'; END;";
                    command.ExecuteNonQuery();
                    Thread.Sleep(500);

                    command.CommandText = $"BEGIN DELETE FROM {TableName}; END;";
                    command.ExecuteNonQuery();
                    Thread.Sleep(500);
                }
            }
        }
    }
}