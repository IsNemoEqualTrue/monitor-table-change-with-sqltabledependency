using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.SqlClient.IntegrationTest.Model;

namespace TableDependency.SqlClient.IntegrationTest
{
    [TestClass]
    public class Merge
    {
        private static string _connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private const string TargetTableName = "energydata";
        private const string SourceTableName = "temp_energydata";

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID(, 'U') IS NOT NULL DROP TABLE [{TargetTableName}];";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"CREATE TABLE {TargetTableName} (id INT, Name NVARCHAR(100), qty INT);";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"IF OBJECT_ID('{SourceTableName}', 'U') IS NOT NULL DROP TABLE [{SourceTableName}];";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"CREATE TABLE {SourceTableName} (id INT, Name NVARCHAR(100), qty INT);";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize()]
        public void TestInitialize()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
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
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
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
                tableDependency = new SqlTableDependency<Check_Model>(_connectionString, TargetTableName);
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


        }

        private void TableDependency_OnError(object sender, ErrorEventArgs e)
        {
            throw e.Error;
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<Check_Model> e)
        {            
        }

        private static void MergeOperation()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    // Synchronize source data with target
                    sqlCommand.CommandText =
                        $"MERGE INTO {TargetTableName} AS target " +
                        $"USING {SourceTableName} AS source " +
                        "   ON target.id = source.id " +
                        "WHEN MATCHED THEN " +
                        "   UPDATE SET target.qty = source.qty " +
                        "WHEN NOT MATCHED BY TARGET THEN " +
                        "   INSERT(id, name, qty) VALUES(source.id, source.name, source.qty) " + 
                        "WHEN NOT MATCHED BY SOURCE THEN " +
                        "   DELETE";

                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);
                }
            }
        }
    }
}