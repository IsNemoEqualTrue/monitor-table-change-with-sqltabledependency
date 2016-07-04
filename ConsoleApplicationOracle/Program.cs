using System;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using Oracle.ManagedDataAccess.Client;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.OracleClient;
using Oracle.ManagedDataAccess.Types;
using ErrorEventArgs = TableDependency.EventArgs.ErrorEventArgs;

namespace ConsoleApplicationOracle
{
    public class DatabaseObjectCleanUpTestOracleModel
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public int Qty { get; set; }
    }

    class Program
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;

        private const string TableName = "AAA";

        internal static void DropAndCreateTable(string connectionString, string tableName)
        {
            using (var connection = new OracleConnection(connectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {tableName.ToUpper()}'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;";
                    command.ExecuteNonQuery();
                    command.CommandText = $"CREATE TABLE {TableName} (ID number(10), NAME varchar2(50), \"Long Description\" varchar2(4000))";
                    command.ExecuteNonQuery();
                }
            }
        }

        static void Main()
        {
            DropAndCreateTable(ConnectionString, TableName);

            var mapper = new ModelToTableMapper<DatabaseObjectCleanUpTestOracleModel>();
            mapper.AddMapping(c => c.Description, "Long Description");

            using (var tableDependency = new OracleTableDependency<DatabaseObjectCleanUpTestOracleModel>(ConnectionString, TableName, mapper))
            {
                tableDependency.OnChanged += Changed;
                tableDependency.TraceLevel = TraceLevel.Verbose;
                tableDependency.TraceListener = new TextWriterTraceListener(File.Create("c:\\temp\\output.txt"));
                tableDependency.OnError += tableDependency_OnError;

                tableDependency.Start();
                Console.WriteLine(@"Waiting for receiving notifications: change some records in the table...");
                Console.WriteLine(@"Press a key to exit");
                Console.ReadKey();
            }
        }

        static void tableDependency_OnError(object sender, ErrorEventArgs e)
        {
            Console.WriteLine("ERROR:");
            Console.WriteLine(e.Error.Message);
            Console.WriteLine(e.Error.StackTrace);
        }

        static void Changed(object sender, RecordChangedEventArgs<DatabaseObjectCleanUpTestOracleModel> e)
        {
            Console.WriteLine(Environment.NewLine);

            if (e.ChangeType != ChangeType.None)
            {
                Console.WriteLine(@"Note: " + e.Entity.Id);
            }
        }
    }
}
