using System;
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
    public class MargeTestOracleModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public int Quantity { get; set; }
    }

    [TestClass]
    public class MergeTestOracle
    {
        private MargeTestOracleModel _modifiedValues;
        private MargeTestOracleModel _insertedValues;

        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static readonly string SourceTableName = "AAAA_Source";
        private static readonly string TarghetTableName = "AAAA_TargetM";
        private static readonly string ProcedureName = "AAAA_MergeTest";

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            OracleHelper.DropProcedure(ConnectionString, ProcedureName);
            OracleHelper.DropTable(ConnectionString, SourceTableName);
            OracleHelper.DropTable(ConnectionString, TarghetTableName);

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {SourceTableName} (ID number(10), NAME varchar2(50), QUANTITY number(10))";
                    command.ExecuteNonQuery();

                    command.CommandText = $"CREATE TABLE {TarghetTableName} (ID number(10), NAME varchar2(50), QUANTITY number(10))";
                    command.ExecuteNonQuery();

                    command.CommandText =
                        $"CREATE PROCEDURE {ProcedureName} AS " + Environment.NewLine +
                        $"BEGIN" + Environment.NewLine +
                        $"MERGE INTO {TarghetTableName.ToUpper()}" + Environment.NewLine +
                        $"USING {SourceTableName.ToUpper()}" + Environment.NewLine +
                        $"ON({SourceTableName.ToUpper()}.ID =  {TarghetTableName.ToUpper()}.ID)" + Environment.NewLine +
                        $"WHEN MATCHED THEN" + Environment.NewLine +
                        $"  UPDATE SET NAME = {SourceTableName.ToUpper()}.NAME, QUANTITY = {SourceTableName.ToUpper()}.QUANTITY" + Environment.NewLine +
                        $"WHEN NOT MATCHED THEN" + Environment.NewLine +
                        $"  INSERT(ID, NAME, QUANTITY) VALUES({SourceTableName.ToUpper()}.ID, {SourceTableName.ToUpper()}.NAME, {SourceTableName.ToUpper()}.QUANTITY);" + Environment.NewLine +
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
                        $"  INSERT INTO {TarghetTableName.ToUpper()} (ID, NAME, QUANTITY) VALUES (0, 'NOT MODIFIED', 0); " + Environment.NewLine +
                        $"  INSERT INTO {TarghetTableName.ToUpper()} (ID, NAME, QUANTITY) VALUES (1, 'UPDATE', 0); " + Environment.NewLine +
                        $"  INSERT INTO {SourceTableName.ToUpper()} (ID, NAME, QUANTITY) VALUES (2, 'INSERT', 100); " + Environment.NewLine +
                        $"  INSERT INTO {SourceTableName.ToUpper()} (ID, NAME, QUANTITY) VALUES (1, 'UPDATE', 200); " + Environment.NewLine +
                        $"  COMMIT;" + Environment.NewLine +
                        $"END;";
                    command.ExecuteNonQuery();
                }
            }
        }

        [TestCategory("Oracle")]
        [TestMethod]
        public void MergeTest()
        {
            OracleTableDependency<MargeTestOracleModel> tableDependency = null;

            try
            {
                tableDependency = new OracleTableDependency<MargeTestOracleModel>(ConnectionString, TarghetTableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.OnError += this.TableDependency_OnError;
                tableDependency.Start();
                Thread.Sleep(1000);

                var t = new Task(MergeOperation);
                t.Start();
                t.Wait(2000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(this._insertedValues.Quantity, 100);
            Assert.AreEqual(this._modifiedValues.Quantity, 200);
        }

        private void TableDependency_OnError(object sender, ErrorEventArgs e)
        {
            throw e.Error;
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<MargeTestOracleModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    this._insertedValues = new MargeTestOracleModel { Id = e.Entity.Id, Name = e.Entity.Name, Quantity = e.Entity.Quantity };
                    break;
                case ChangeType.Update:
                    this._modifiedValues = new MargeTestOracleModel { Id = e.Entity.Id, Name = e.Entity.Name, Quantity = e.Entity.Quantity };
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
                }

                Thread.Sleep(5000);
            }
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            OracleHelper.DropProcedure(ConnectionString, ProcedureName);
            OracleHelper.DropTable(ConnectionString, SourceTableName);
            OracleHelper.DropTable(ConnectionString, TarghetTableName);
        }
    }
}