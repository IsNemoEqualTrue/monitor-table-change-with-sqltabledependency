using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
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
    public class AAA_Item3
    {
        public long Id { get; set; }
        public string Name { get; set; }
        [Column(ColumnName)]
        public string FamilyName { get; set; }
        private const string ColumnName = "SURNAME";
        public static string GetColumnName => ColumnName;
    }

    [TestClass]
    public class TableNameFromModelClassNameAndUpdateOfTestOracle
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static readonly string TableName = typeof(AAA_Item3).Name.ToUpper();       
        private static readonly Dictionary<string, Tuple<AAA_Item3, AAA_Item3>> CheckValues = new Dictionary<string, Tuple<AAA_Item3, AAA_Item3>>();
        private static int _counter = 0;

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            OracleHelper.DropTable(ConnectionString, TableName);

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {TableName} (ID NUMBER(10), NAME VARCHAR2(50), SURNAME VARCHAR2(4000))";
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
            OracleTableDependency<AAA_Item3> tableDependency = null;
            string naming = null;

            try
            {
                UpdateOfModel<AAA_Item3> updateOF = new UpdateOfModel<AAA_Item3>();
                updateOF.Add(model => model.FamilyName);

                tableDependency = new OracleTableDependency<AAA_Item3>(ConnectionString, updateOF);
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
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.FamilyName, CheckValues[ChangeType.Insert.ToString()].Item1.FamilyName);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.FamilyName, CheckValues[ChangeType.Delete.ToString()].Item1.FamilyName);

            Assert.IsTrue(OracleHelper.AreAllDbObjectsDisposed(ConnectionString, naming));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<AAA_Item3> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.Id = e.Entity.Id;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Insert.ToString()].Item2.FamilyName = e.Entity.FamilyName;
                    break;

                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.Id = e.Entity.Id;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Delete.ToString()].Item2.FamilyName = e.Entity.FamilyName;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<AAA_Item3, AAA_Item3>(new AAA_Item3 { Id = 23, Name = "Pizza Mergherita", FamilyName = "Pizza Mergherita" }, new AAA_Item3()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<AAA_Item3, AAA_Item3>(new AAA_Item3 { Id = 23, Name = "Pizza Funghi", FamilyName = "Pizza Mergherita" }, new AAA_Item3()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<AAA_Item3, AAA_Item3>(new AAA_Item3 { Id = 23, Name = "Pizza Funghi", FamilyName = "Pizza Mergherita" }, new AAA_Item3()));

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"BEGIN INSERT INTO {TableName} (ID, NAME, SURNAME) VALUES ({CheckValues[ChangeType.Insert.ToString()].Item1.Id}, '{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', '{CheckValues[ChangeType.Insert.ToString()].Item1.FamilyName}'); END;";
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