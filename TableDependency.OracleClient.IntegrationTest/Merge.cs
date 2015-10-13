using System;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.DataAccess.Client;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.OracleClient.IntegrationTest.Model;

namespace TableDependency.OracleClient.IntegrationTest
{
    [TestClass]
    public class Merge
    {
        private Item modifiedValues;
        private Item insertedValues;

        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private static readonly string SourceTableName = "AAAA_Source";
        private static readonly string TarghetTableName = "AAAA_TargetM";
        private static readonly string ProcedureName = "AAAA_MergeTest";

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            DropProcedure(ConnectionString, ProcedureName);
            DropTable(ConnectionString, SourceTableName);
            DropTable(ConnectionString, TarghetTableName);

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {SourceTableName} (ID number(10), NAME varchar2(50), qty number(10))";
                    command.ExecuteNonQuery();

                    command.CommandText = $"CREATE TABLE {TarghetTableName} (ID number(10), NAME varchar2(50), qty number(10))";
                    command.ExecuteNonQuery();

                    command.CommandText =
                        $"CREATE PROCEDURE {ProcedureName} AS " + Environment.NewLine +
                        $"BEGIN" + Environment.NewLine +
                        $"MERGE into {TarghetTableName.ToUpper()}" + Environment.NewLine +
                        $"USING {SourceTableName.ToUpper()}" + Environment.NewLine +
                        $"ON({SourceTableName.ToUpper()}.id =  {TarghetTableName.ToUpper()}.id)" + Environment.NewLine +
                        $"WHEN MATCHED THEN" + Environment.NewLine +
                        $"  UPDATE SET name = {SourceTableName.ToUpper()}.name, qty = {SourceTableName.ToUpper()}.qty" + Environment.NewLine +
                        $"WHEN NOT MATCHED THEN" + Environment.NewLine +
                        $"  INSERT(id, name, qty) VALUES({SourceTableName.ToUpper()}.id, {SourceTableName.ToUpper()}.name, {SourceTableName.ToUpper()}.qty);" + Environment.NewLine +
                        $"END;";
                    command.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize()]
        public void TestInitialize()
        {
            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $"BEGIN " +
                        $"  INSERT INTO {TarghetTableName.ToUpper()} (ID, NAME, QTY) VALUES (0, 'NOT MODIFIED', 0); " + Environment.NewLine +
                        $"  INSERT INTO {TarghetTableName.ToUpper()} (ID, NAME, QTY) VALUES (1, 'UPDATE', 0); " + Environment.NewLine +
                        $"  INSERT INTO {SourceTableName.ToUpper()} (ID, NAME, QTY) VALUES (2, 'INSERT', 100); " + Environment.NewLine +
                        $"  INSERT INTO {SourceTableName.ToUpper()} (ID, NAME, QTY) VALUES (1, 'UPDATE', 200); " + Environment.NewLine +
                        $"  COMMIT;" + Environment.NewLine +
                        $"END;";
                    command.ExecuteNonQuery();
                }
            }
        }

        [TestMethod]
        public void MergeTest()
        {
            OracleTableDependency<Item> tableDependency = null;

            try
            {
                tableDependency = new OracleTableDependency<Item>(ConnectionString, TarghetTableName);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.OnError += TableDependency_OnError;
                tableDependency.Start();

                Thread.Sleep(10000);

                var t = new Task(MergeOperation);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(insertedValues.qty, 100);
            Assert.AreEqual(modifiedValues.qty, 200);
        }

        private void TableDependency_OnError(object sender, ErrorEventArgs e)
        {
            throw e.Error;
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<Item> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    insertedValues = new Item { Id = e.Entity.Id, Name = e.Entity.Name, qty = e.Entity.qty };
                    break;
                case ChangeType.Update:
                    modifiedValues = new Item { Id = e.Entity.Id, Name = e.Entity.Name, qty = e.Entity.qty };
                    break;
            }
        }

        private static void MergeOperation()
        {
            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    // Synchronize source data with target
                    command.CommandType = System.Data.CommandType.StoredProcedure;
                    command.CommandText = ProcedureName;
                    command.ExecuteNonQuery();
                    Thread.Sleep(500);
                }
            }
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            DropProcedure(ConnectionString, ProcedureName);
            DropTable(ConnectionString, SourceTableName);
            DropTable(ConnectionString, TarghetTableName);
        }

        private static void DropTable(string connectionString, string tableName)
        {
            using (var connection = new OracleConnection(connectionString))
            {
                connection.Open();
                using (var sqlCommand = connection.CreateCommand())
                {
                    sqlCommand.CommandText = $"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {tableName.ToUpper()}'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        private static void DropProcedure(string connectionString, string procedureName)
        {
            using (var connection = new OracleConnection(connectionString))
            {
                connection.Open();
                using (var sqlCommand = connection.CreateCommand())
                {
                    sqlCommand.CommandText = $"BEGIN EXECUTE IMMEDIATE 'DROP PROCEDURE {procedureName.ToUpper()}'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}