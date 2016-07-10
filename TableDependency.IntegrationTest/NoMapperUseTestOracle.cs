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
using TableDependency.OracleClient;

namespace TableDependency.IntegrationTest
{
    public class AAAItem2
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
    }

    [TestClass]
    public class NoMapperUseTestOracle
    {
        private static string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static int _counter;
        private static readonly Dictionary<string, Tuple<AAAItem2, AAAItem2>> CheckValues = new Dictionary<string, Tuple<AAAItem2, AAAItem2>>();
        private static readonly string TableName = typeof(AAAItem2).Name.ToUpper();

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

        [TestCategory("Oracle")]
        [TestMethod]
        public void EventForAllColumnsTest()
        {
            OracleTableDependency<AAAItem2> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<AAAItem2>(ConnectionString, TableName);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(1000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(2000);
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
            Assert.IsTrue(OracleHelper.AreAllDbObjectsDisposed(ConnectionString, naming));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<AAAItem2> e)
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
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<AAAItem2, AAAItem2>(new AAAItem2 { Name = "Christian", Surname = "Del Bianco" }, new AAAItem2()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<AAAItem2, AAAItem2>(new AAAItem2 { Name = "Velia", Surname = "Ceccarelli" }, new AAAItem2()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<AAAItem2, AAAItem2>(new AAAItem2 { Name = "Velia", Surname = "Ceccarelli" }, new AAAItem2()));

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