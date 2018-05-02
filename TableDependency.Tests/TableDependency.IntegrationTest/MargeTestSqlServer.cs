using System;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Base;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    public class MargeTestSqlServerModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
        public DateTime Born { get; set; }
        public int Quantity { get; set; }
    }

    [TestClass]
    public class MargeTestSqlServer : SqlTableDependencyBaseTest
    {
        private MargeTestSqlServerModel _modifiedValues;
        private MargeTestSqlServerModel _insertedValues;
        private MargeTestSqlServerModel _deletedValues;

        private const string TargetTableName = "energydata";
        private const string SourceTableName = "temp_energydata";

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TargetTableName}', 'U') IS NOT NULL DROP TABLE [{TargetTableName}];";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"CREATE TABLE {TargetTableName} (Id INT, Name NVARCHAR(100), quantity INT);";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"IF OBJECT_ID('{SourceTableName}', 'U') IS NOT NULL DROP TABLE [{SourceTableName}];";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"CREATE TABLE {SourceTableName} (Id INT, Name NVARCHAR(100), quantity INT);";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = "IF EXISTS (SELECT * FROM sys.objects WHERE name = N'testMerge') DROP PROCEDURE[testMerge]";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText =
                        "CREATE PROCEDURE dbo.testMerge AS " + Environment.NewLine +
                        "BEGIN " + Environment.NewLine +
                        "  SET NOCOUNT ON; " + Environment.NewLine +
                        $"  MERGE INTO {TargetTableName} AS target " + Environment.NewLine +
                        $"  USING {SourceTableName} AS source ON target.Id = source.Id " + Environment.NewLine +
                        "  WHEN MATCHED THEN UPDATE SET target.quantity = source.quantity " + Environment.NewLine +
                        "  WHEN NOT MATCHED BY TARGET THEN INSERT(Id, Name, quantity) VALUES(source.Id, source.Name, source.quantity) " + Environment.NewLine +
                        "  WHEN NOT MATCHED BY SOURCE THEN DELETE; " + Environment.NewLine +
                        "END;";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize()]
        public void TestInitialize()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"insert into {TargetTableName} (id, name, quantity) values (0, 'DELETE', 0);";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"insert into {TargetTableName} (id, name, quantity) values (1, 'UPDATE', 0);";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"insert into {SourceTableName} (id, name, quantity) values (2, 'INSERT', 100);";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"insert into {SourceTableName} (id, name, quantity) values (1, 'UPDATE', 200);";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
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

        [TestCategory("SqlServer")]
        [TestMethod]
        public void MergeTest()
        {
            SqlTableDependency<MargeTestSqlServerModel> tableDependency = null;

            try
            {
                tableDependency = new SqlTableDependency<MargeTestSqlServerModel>(ConnectionStringForTestUser, tableName: TargetTableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.OnError += this.TableDependency_OnError;
                tableDependency.Start();

                Thread.Sleep(10000);

                var t = new Task(MergeOperation);
                t.Start();
                Thread.Sleep(1000 * 60 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(this._insertedValues.Quantity, 100);
            Assert.AreEqual(this._modifiedValues.Quantity, 200);
            Assert.AreEqual(this._deletedValues.Quantity, 0);
        }

        private void TableDependency_OnError(object sender, ErrorEventArgs e)
        {
            Assert.Fail(e.Error.Message);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<MargeTestSqlServerModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    this._insertedValues = new MargeTestSqlServerModel { Id = e.Entity.Id, Name = e.Entity.Name, Quantity = e.Entity.Quantity };
                    break;
                case ChangeType.Update:
                    this._modifiedValues = new MargeTestSqlServerModel { Id = e.Entity.Id, Name = e.Entity.Name, Quantity = e.Entity.Quantity };
                    break;
                case ChangeType.Delete:
                    this._deletedValues = new MargeTestSqlServerModel { Id = e.Entity.Id, Name = e.Entity.Name, Quantity = e.Entity.Quantity };
                    break;
            }
        }

        private static void MergeOperation()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    // Synchronize source data with target
                    sqlCommand.CommandType = System.Data.CommandType.StoredProcedure;
                    sqlCommand.CommandText = "testMerge";
                    sqlCommand.ExecuteNonQuery();                    
                }
            }

            Thread.Sleep(500);
        }
    }
}