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
    public class AAAB_Table
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public int Qty { get; set; }
    }

    [TestClass]
    public class NoTableAndColumnDefinitionsTestOracleServer
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static int _counter = 0;
        private static readonly Dictionary<string, Tuple<AAAB_Table, AAAB_Table>> CheckValues = new Dictionary<string, Tuple<AAAB_Table, AAAB_Table>>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            OracleHelper.DropTable(ConnectionString, "AAAB_Table");

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE AAAB_TABLE (ID NUMBER(10), NAME VARCHAR2(50), DESCRIPTION VARCHAR2(4000))";
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
            OracleHelper.DropTable(ConnectionString, "AAAB_Table");
        }

        [TestCategory("Oracle")]
        [TestMethod]
        public void Test()
        {
            OracleTableDependency<AAAB_Table> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new OracleTableDependency<AAAB_Table>(ConnectionString);
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

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Id, CheckValues[ChangeType.Insert.ToString()].Item1.Id);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Description, CheckValues[ChangeType.Insert.ToString()].Item1.Description);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Id, CheckValues[ChangeType.Update.ToString()].Item1.Id);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, CheckValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Description, CheckValues[ChangeType.Update.ToString()].Item1.Description);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Id, CheckValues[ChangeType.Delete.ToString()].Item1.Id);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Description, CheckValues[ChangeType.Delete.ToString()].Item1.Description);

            Assert.IsTrue(OracleHelper.AreAllDbObjectsDisposed(ConnectionString, naming));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<AAAB_Table> e)
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
                    CheckValues[ChangeType.Update.ToString()].Item2.Id = e.Entity.Id;
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
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<AAAB_Table, AAAB_Table>(new AAAB_Table { Id = 23, Name = "Pizza Mergherita", Description = "Pizza Mergherita" }, new AAAB_Table()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<AAAB_Table, AAAB_Table>(new AAAB_Table { Id = 23, Name = "Pizza Funghi", Description = "Pizza Funghi" }, new AAAB_Table()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<AAAB_Table, AAAB_Table>(new AAAB_Table { Id = 23, Name = "Pizza Funghi", Description = "Pizza Funghi" }, new AAAB_Table()));

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"BEGIN INSERT INTO AAAB_TABLE (ID, NAME, DESCRIPTION) VALUES ({CheckValues[ChangeType.Insert.ToString()].Item1.Id}, '{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', '{CheckValues[ChangeType.Insert.ToString()].Item1.Description}'); END;";
                    command.ExecuteNonQuery();
                    Thread.Sleep(2000);

                    command.CommandText = $"BEGIN UPDATE AAAB_TABLE  SET NAME = '{CheckValues[ChangeType.Update.ToString()].Item1.Name}', DESCRIPTION = '{CheckValues[ChangeType.Update.ToString()].Item1.Description}'; END;";
                    command.ExecuteNonQuery();
                    Thread.Sleep(2000);

                    command.CommandText = $"BEGIN DELETE FROM AAAB_TABLE; END;";
                    command.ExecuteNonQuery();
                    Thread.Sleep(2000);
                }
            }
        }
    }
}