using System;
using Oracle.ManagedDataAccess.Client;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.OracleClient;

namespace ApplicationListener1
{
    class Program
    {
        private const string ConnectionString = "Data Source = " +
                                        "(DESCRIPTION = " +
                                        " (ADDRESS_LIST = " +
                                        " (ADDRESS = (PROTOCOL = TCP)" +
                                        " (HOST = 127.0.0.1) " +
                                        " (PORT = 1521) " +
                                        " )" +
                                        " )" +
                                        " (CONNECT_DATA = " +
                                        " (SERVICE_NAME = XE)" +
                                        " )" +
                                        ");" +
                                        "User Id=SYSTEM;" +
                                        "password=tiger;";

        private const string TableName = "PRODUCTS";
        static int deletedCnt = 0;
        static int insertedCnt = 0;
        static int updatedCnt = 0;

        internal static void DropAndCreateTable(string connectionString, string tableName)
        {
            using (var connection = new OracleConnection(connectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {tableName.ToUpper()}'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;";
                    command.ExecuteNonQuery();               
                    command.CommandText = $"CREATE TABLE {tableName} (ID VARCHAR2(10), NAME VARCHAR2(50), DESCRIPTION VARCHAR2(4000))";
                    command.ExecuteNonQuery();
                }
            }
        }

        static void Main()
        {
            DropAndCreateTable(ConnectionString, "PRODUCTS");

            using (var tableDependency = new OracleTableDependency<Product>(ConnectionString, TableName))
            {
                tableDependency.OnChanged += Changed;
                tableDependency.OnError += tableDependency_OnError;

                tableDependency.Start();
                Console.WriteLine(@"Waiting for receiving notifications: change some records in the table...");
                Console.WriteLine(@"Press a key to exit");
                Console.ReadKey();
            }
        }

        static void tableDependency_OnError(object sender, ErrorEventArgs e)
        {
            Console.WriteLine(e.Error.Message);
            Console.WriteLine(e.Error.StackTrace);
        }

        static void Changed(object sender, RecordChangedEventArgs<Product> e)
        {
            Console.WriteLine(Environment.NewLine);

            if (e.ChangeType != ChangeType.None)
            {
                if (e.ChangeType == ChangeType.Insert) insertedCnt++;
                if (e.ChangeType == ChangeType.Update) updatedCnt++;
                if (e.ChangeType == ChangeType.Delete) deletedCnt++;

                var changedEntity = e.Entity;
                Console.WriteLine("DML operation: " + e.ChangeType);
                Console.WriteLine("ID: " + changedEntity.Id);
                Console.WriteLine("Name: " + changedEntity.Name);
                Console.WriteLine("Long Description: " + changedEntity.Description);
                Console.WriteLine("insert: " + insertedCnt + " / update: " + updatedCnt + " / delete: " + deletedCnt);
            }
        }
    }
}