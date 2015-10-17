using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Models;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    [TestClass]
    public class MargeTestSqlServer
    {
        private Check_Model _modifiedValues;
        private Check_Model _insertedValues;
        private Check_Model _deletedValues;

        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServerConnectionString"].ConnectionString;
        private const string TargetTableName = "energydata";
        private const string SourceTableName = "temp_energydata";

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TargetTableName}', 'U') IS NOT NULL DROP TABLE [{TargetTableName}];";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"CREATE TABLE {TargetTableName} (Id INT, Name NVARCHAR(100), qty INT);";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"IF OBJECT_ID('{SourceTableName}', 'U') IS NOT NULL DROP TABLE [{SourceTableName}];";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"CREATE TABLE {SourceTableName} (Id INT, Name NVARCHAR(100), qty INT);";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = "IF EXISTS (SELECT * FROM sys.objects WHERE name = N'testMerge') DROP PROCEDURE[testMerge]";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText =
                        "CREATE PROCEDURE dbo.testMerge AS " + Environment.NewLine +
                        "BEGIN " + Environment.NewLine +
                        "  SET NOCOUNT ON; " + Environment.NewLine +
                        $"  MERGE INTO {TargetTableName} AS target " + Environment.NewLine +
                        $"  USING {SourceTableName} AS source ON target.Id = source.Id " + Environment.NewLine +
                        "  WHEN MATCHED THEN UPDATE SET target.qty = source.qty " + Environment.NewLine +
                        "  WHEN NOT MATCHED BY TARGET THEN INSERT(Id, Name, qty) VALUES(source.Id, source.Name, source.qty) " + Environment.NewLine +
                        "  WHEN NOT MATCHED BY SOURCE THEN DELETE; " + Environment.NewLine +
                        "END;";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize()]
        public void TestInitialize()
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"insert into {TargetTableName} (id, name, qty) values (0, 'DELETE', 0);";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"insert into {TargetTableName} (id, name, qty) values (1, 'UPDATE', 0);";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"insert into {SourceTableName} (id, name, qty) values (2, 'INSERT', 100);";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"insert into {SourceTableName} (id, name, qty) values (1, 'UPDATE', 200);";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = "IF EXISTS (SELECT * FROM sys.objects WHERE name = N'testMerge') DROP PROCEDURE[testMerge]";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TargetTableName}', 'U') IS NOT NULL DROP TABLE [{TargetTableName}];";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"IF OBJECT_ID('{SourceTableName}', 'U') IS NOT NULL DROP TABLE [{SourceTableName}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestMethod]
        public void MergeTest()
        {
            SqlTableDependency<Check_Model> tableDependency = null;

            try
            {
                tableDependency = new SqlTableDependency<Check_Model>(ConnectionString, TargetTableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.OnError += this.TableDependency_OnError;
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

            Assert.AreEqual(this._insertedValues.qty, 100);
            Assert.AreEqual(this._modifiedValues.qty, 200);
            Assert.AreEqual(this._deletedValues.qty, 0);
        }

        private void TableDependency_OnError(object sender, ErrorEventArgs e)
        {
            throw e.Error;
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<Check_Model> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    this._insertedValues = new Check_Model { Id = e.Entity.Id, Name = e.Entity.Name, qty = e.Entity.qty };
                    break;
                case ChangeType.Update:
                    this._modifiedValues = new Check_Model { Id = e.Entity.Id, Name = e.Entity.Name, qty = e.Entity.qty };
                    break;
                case ChangeType.Delete:
                    this._deletedValues = new Check_Model { Id = e.Entity.Id, Name = e.Entity.Name, qty = e.Entity.qty };
                    break;
            }
        }

        private static void MergeOperation()
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    // Synchronize source data with target
                    sqlCommand.CommandType = System.Data.CommandType.StoredProcedure;
                    sqlCommand.CommandText = "testMerge";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);
                }
            }
        }
    }
}