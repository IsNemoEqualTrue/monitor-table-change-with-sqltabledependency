using System;
using System.Configuration;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient;
using ErrorEventArgs = TableDependency.EventArgs.ErrorEventArgs;

namespace ConsoleApplicationSqlServer
{
    class Program
    {
        private static void Main()
        {
            var connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;

            using (var tableDependency = new SqlTableDependency<Customer>(connectionString, "Customers"))
            {
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.OnError += TableDependency_OnError;
                tableDependency.Start();

                Console.WriteLine(@"Waiting for receiving notifications...");
                Console.WriteLine(@"Press a key to stop");
                Console.ReadKey();

                tableDependency.Stop();
            }

            Console.WriteLine(@"I ended withour error.");
        }

        private static void TableDependency_OnError(object sender, ErrorEventArgs e)
        {
            Console.WriteLine(e.Error.Message);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Customer> e)
        {
            Console.WriteLine(Environment.NewLine);

            if (e.ChangeType != ChangeType.None)
            {
                var changedEntity = e.Entity;
                Console.WriteLine(@"DML operation: " + e.ChangeType);
                Console.WriteLine(@"CompanyName: " + changedEntity.CompanyName);
                Console.WriteLine(@"ContactName: " + changedEntity.ContactName);
                Console.WriteLine(@"CustomerID: " + changedEntity.CustomerID);
            }
        }
    }
}