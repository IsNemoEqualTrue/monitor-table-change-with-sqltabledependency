using System;
using LoadTests.Models;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.SqlClient;

namespace LoadTests.Sql.Listener1
{
    class Program
    {
        static void Main()
        {
            Console.Title = new string('*', 10) + " SqlTableDependency Listener 1 " + new string('*', 10);

            var connectionString = "data source=.;initial catalog=TableDependencyDB;integrated security=True";

            var mapper = new ModelToTableMapper<Customer>();
            mapper.AddMapping(c => c.Name, "First Name").AddMapping(c => c.Surname, "Second Name");

            using (var tableDependency = new SqlTableDependency<Customer>(connectionString, "Customers", mapper))
            {
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.OnError += TableDependency_OnError;

                tableDependency.Start();
                Console.WriteLine("Waiting for receiving notifications...");
                Console.WriteLine("Press a key to stop");
                Console.ReadKey();
                tableDependency.Stop();
            }
        }

        static void TableDependency_OnError(object sender, ErrorEventArgs e)
        {
            Console.WriteLine(e.Error.Message);
        }

        static void TableDependency_Changed(object sender, RecordChangedEventArgs<Customer> e)
        {
            Console.WriteLine(Environment.NewLine);

            if (e.ChangeType != ChangeType.None)
            {
                var changedEntity = e.Entity;
                Console.WriteLine("DML operation: " + e.ChangeType);
                Console.WriteLine("ID: " + changedEntity.Id);
                Console.WriteLine("Name: " + changedEntity.Name);
                Console.WriteLine("Surname: " + changedEntity.Surname);
            }
        }
    }
}