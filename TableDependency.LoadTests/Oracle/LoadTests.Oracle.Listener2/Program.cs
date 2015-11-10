using System;
using LoadTests.Models;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.OracleClient;

namespace LoadTests.Oracle.Listener2
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

        static void Main()
        {
            Console.Title = new string('*', 10) + " OracleTableDependency Listener 2 " + new string('*', 10);

            var mapper = new ModelToTableMapper<Product>();
            mapper.AddMapping(c => c.Description, "Long Description");

            using (var tableDependency = new OracleTableDependency<Product>(
                ConnectionString,
                TableName,
                mapper: mapper))
            {
                tableDependency.OnChanged += Changed;
                tableDependency.OnError += tableDependency_OnError;

                tableDependency.Start();
                Console.WriteLine("Waiting for receiving notifications: change some records in the table...");
                Console.WriteLine("Press a key to exit");
                Console.ReadKey();
            }
        }

        static void tableDependency_OnError(object sender, ErrorEventArgs e)
        {
            Console.WriteLine(e.Error.Message);
        }

        static void Changed(object sender, RecordChangedEventArgs<Product> e)
        {
            Console.WriteLine(Environment.NewLine);
            Console.WriteLine(new String('*', 60));

            if (e.ChangeType != ChangeType.None)
            {
                var changedEntity = e.Entity;
                Console.WriteLine("DML operation: " + e.ChangeType);
                Console.WriteLine("ID: " + changedEntity.Id);
                Console.WriteLine("Name: " + changedEntity.Name);
                Console.WriteLine("Long Description: " + changedEntity.Description);
                Console.WriteLine(Environment.NewLine);
            }

            Console.WriteLine(new String('*', 60));
            Console.WriteLine("Press a key to exit");

        }
    }
}