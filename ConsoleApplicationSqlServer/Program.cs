using System;
using System.Configuration;
using System.Data.SqlClient;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.SqlClient;

namespace ConsoleApplicationSqlServer
{
    class Program
    {
        private static void Main()
        {
            var connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;

            using (var tableDependency = new SqlTableDependency<Customer>(connectionString, "Customer"))
            {
                tableDependency.OnStatusChanged += TableDependency_OnStatusChanged;
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.OnError += TableDependency_OnError;

                tableDependency.Start();
                Console.WriteLine(@"Waiting for receiving notifications...");
                Console.WriteLine(@"Press a key to stop");
                Console.ReadKey();
                tableDependency.Stop();
            }
        }

        private static void TableDependency_OnStatusChanged(object sender, StatusChangedEventArgs e)
        {
            Console.WriteLine("----------> " + e.Status);
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
                Console.WriteLine(@"id: " + changedEntity.Id);
                Console.WriteLine(@"Name: " + changedEntity.Name);
                Console.WriteLine(@"Surname: " + changedEntity.Surname);
            }
        }
    }
}