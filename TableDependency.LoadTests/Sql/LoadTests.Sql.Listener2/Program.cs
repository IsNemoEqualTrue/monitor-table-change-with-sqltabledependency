using System;
using LoadTests.Models;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.SqlClient;

namespace LoadTests.Sql.Listener2
{
    class Program
    {
        private const string ConnectionString = "data source=.;initial catalog=TableDependencyDB;integrated security=True";
        private const string TableName = "Customers";

        static void Main()
        {
            Console.Title = new string('*', 10) + " SqlTableDependency Listener 2 " + new string('*', 10);

            var mapper = new ModelToTableMapper<Customer>();
            mapper.AddMapping(c => c.Name, "First Name").AddMapping(c => c.Surname, "Second Name");

            using (var tableDependency = new SqlTableDependency<Customer>(ConnectionString, TableName, mapper))
            {
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.OnError += TableDependency_OnError;

                tableDependency.Start();
                Console.WriteLine("Waiting for receiving notifications: change some records in the table...");
                Console.WriteLine("Press a key to exit");
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
            Console.WriteLine(new string('*', 60));

            if (e.ChangeType != TableDependency.Enums.ChangeType.None)
            {
                var changedEntity = e.Entity;
                Console.WriteLine("DML operation: " + e.ChangeType);
                Console.WriteLine("ID: " + changedEntity.Id);
                Console.WriteLine("Name: " + changedEntity.Name);
                Console.WriteLine("Surname: " + changedEntity.Surname);
                Console.WriteLine(Environment.NewLine);
            }
        }
    }
}