using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.ManagedDataAccess.Client;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.Mappers;
using TableDependency.OracleClient;

namespace TableDependency.IntegrationTest
{
    public class DisposeAndRestartWithSameObjectsTestOracleModel
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public int Qty { get; set; }
    }

    [TestClass]
    public class DisposeAndRestartWithSameObjectsTestOracle
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static string TableName = "AAAItem".ToUpper();
        private static int _counter;
        private static Dictionary<string, Tuple<DisposeAndRestartWithSameObjectsTestOracleModel, DisposeAndRestartWithSameObjectsTestOracleModel>> _checkValues = new Dictionary<string, Tuple<DisposeAndRestartWithSameObjectsTestOracleModel, DisposeAndRestartWithSameObjectsTestOracleModel>>();

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

        [TestCategory("Oracle")]
        [TestMethod]
        public void Test()
        {
            var namingToUse = "AACustomNaming".ToUpper();

            var mapper = new ModelToTableMapper<DisposeAndRestartWithSameObjectsTestOracleModel>();
            mapper.AddMapping(c => c.Description, "Long Description");

            using (var tableDependency = new OracleTableDependency<DisposeAndRestartWithSameObjectsTestOracleModel>(ConnectionString, TableName, mapper, false))
            {
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                namingToUse = tableDependency.DataBaseObjectsNamingConvention;
                Thread.Sleep(1 * 25 * 1000);
            }

            Thread.Sleep(1 * 60 * 1000);

            using (var tableDependency = new OracleTableDependency<DisposeAndRestartWithSameObjectsTestOracleModel>(ConnectionString, TableName, mapper, true, namingToUse))
            {
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                Assert.AreEqual(tableDependency.DataBaseObjectsNamingConvention, namingToUse);

                Thread.Sleep(1 * 25 * 1000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(3 * 60 * 1000);
            }

            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, namingToUse));
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Name, _checkValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Description, _checkValues[ChangeType.Insert.ToString()].Item1.Description);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Name, _checkValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Description, _checkValues[ChangeType.Update.ToString()].Item1.Description);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Name, _checkValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Description, _checkValues[ChangeType.Delete.ToString()].Item1.Description);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<DisposeAndRestartWithSameObjectsTestOracleModel> e)
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
            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<DisposeAndRestartWithSameObjectsTestOracleModel, DisposeAndRestartWithSameObjectsTestOracleModel>(new DisposeAndRestartWithSameObjectsTestOracleModel { Name = "Christian", Description = "Del Bianco" }, new DisposeAndRestartWithSameObjectsTestOracleModel()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<DisposeAndRestartWithSameObjectsTestOracleModel, DisposeAndRestartWithSameObjectsTestOracleModel>(new DisposeAndRestartWithSameObjectsTestOracleModel { Name = "Velia", Description = "Ceccarelli" }, new DisposeAndRestartWithSameObjectsTestOracleModel()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<DisposeAndRestartWithSameObjectsTestOracleModel, DisposeAndRestartWithSameObjectsTestOracleModel>(new DisposeAndRestartWithSameObjectsTestOracleModel { Name = "Velia", Description = "Ceccarelli" }, new DisposeAndRestartWithSameObjectsTestOracleModel()));

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"BEGIN INSERT INTO {TableName} (ID, NAME, \"Long Description\") VALUES ({_checkValues[ChangeType.Insert.ToString()].Item1.Id}, '{_checkValues[ChangeType.Insert.ToString()].Item1.Name}', '{_checkValues[ChangeType.Insert.ToString()].Item1.Description}'); END;";
                    command.ExecuteNonQuery();
                    Thread.Sleep(2000);

                    command.CommandText = $"BEGIN UPDATE {TableName} SET NAME = '{_checkValues[ChangeType.Update.ToString()].Item1.Name}', \"Long Description\" = '{_checkValues[ChangeType.Update.ToString()].Item1.Description}'; END;";
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