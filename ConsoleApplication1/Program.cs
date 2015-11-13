using System;
using TableDependency.EventArgs;
using TableDependency.OracleClient;

namespace ConsoleApplication1
{
    public class Product
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
    }

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

        static void Main(string[] args)
        {
            using (var tableDependency = new OracleTableDependency<Product>(ConnectionString, TableName))
            {
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.OnError += tableDependency_OnError;
                tableDependency.Start();

                Console.WriteLine("press a key");
                Console.ReadKey();
            }

            Console.WriteLine("Finito....");
            Console.ReadKey();
        }

        static void tableDependency_OnError(object sender, ErrorEventArgs e)
        {
            Console.WriteLine(e.Error.Message);
        }

        static void TableDependency_Changed(object sender, RecordChangedEventArgs<Product> e)
        {
            Console.WriteLine(Environment.NewLine);

            var changedEntity = e.Entity;
            Console.WriteLine("DML operation: " + e.ChangeType);
            Console.WriteLine("ID: " + changedEntity.Id);
            Console.WriteLine("Name: " + changedEntity.Name);
            Console.WriteLine("Long Description: " + changedEntity.Description);
        }
    }
}