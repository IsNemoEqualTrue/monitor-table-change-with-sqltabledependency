using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
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
    [Table("AAWItemsTable")]
    public class Item5
    {
        public long Id { get; set; }
        public string Name { get; set; }
        [Column("Long Description")]
        public string Infos { get; set; }
    }

    [TestClass]
    public class ModelWithAnnotationUsedWithCunstructorParameterTestOracle
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static readonly string TableName = "AAAA";
        private static int _counter;
        private static readonly Dictionary<string, Item5> CheckValues = new Dictionary<string, Item5>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            OracleHelper.DropTable(ConnectionString, TableName);

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {TableName} (ID number(10), NAME varchar2(50), \"More Info\" varchar2(4000))";
                    command.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize()]
        public void TestInitialize()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Item5());
            CheckValues.Add(ChangeType.Update.ToString(), new Item5());
            CheckValues.Add(ChangeType.Delete.ToString(), new Item5());
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            OracleHelper.DropTable(ConnectionString, TableName);
        }

        [TestMethod]
        public void Test()
        {
            OracleTableDependency<Item5> tableDependency = null;
            string naming = null;

            var mapper = new ModelToTableMapper<Item5>();
            mapper.AddMapping(c => c.Infos, "More Info");

            var updateOf = new List<string>();
            updateOf.Add("More Info");

            try
            {
                tableDependency = new OracleTableDependency<Item5>(ConnectionString, TableName, mapper, updateOf);
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

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Name, "Pizza MERGHERITA");
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Infos, "Pizza MERGHERITA");

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Name, "Pizza MERGHERITA");
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Infos, "FUNGHI PORCINI");

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Name, "Pizza");
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Infos, "FUNGHI PORCINI");

            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Item5> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _counter++;
                    CheckValues[ChangeType.Insert.ToString()].Name = e.Entity.Name;
                    CheckValues[ChangeType.Insert.ToString()].Infos = e.Entity.Infos;
                    break;

                case ChangeType.Update:
                    _counter++;
                    CheckValues[ChangeType.Update.ToString()].Name = e.Entity.Name;
                    CheckValues[ChangeType.Update.ToString()].Infos = e.Entity.Infos;
                    break;

                case ChangeType.Delete:
                    _counter++;
                    CheckValues[ChangeType.Delete.ToString()].Name = e.Entity.Name;
                    CheckValues[ChangeType.Delete.ToString()].Infos = e.Entity.Infos;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"BEGIN INSERT INTO {TableName} (ID, NAME, \"More Info\") VALUES (1, 'Pizza MERGHERITA', 'Pizza MERGHERITA'); END;";
                    command.ExecuteNonQuery();
                    Thread.Sleep(2000);

                    command.CommandText = $"BEGIN UPDATE {TableName} SET \"More Info\" = 'FUNGHI PORCINI'; END;";
                    command.ExecuteNonQuery();
                    Thread.Sleep(2000);

                    command.CommandText = $"BEGIN UPDATE {TableName} SET NAME = 'Pizza'; END;";
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