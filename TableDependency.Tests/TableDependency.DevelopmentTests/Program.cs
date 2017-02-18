using System;
using System.Configuration;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.SqlClient;
using ErrorEventArgs = TableDependency.EventArgs.ErrorEventArgs;

namespace ConsoleApplicationSqlServer
{
    public partial class Program
    {
        private static void Main()
        {          
            var connectionString = ConfigurationManager.ConnectionStrings["WIASqlServerConnectionString"].ConnectionString;

            var mapper = new ModelToTableMapper<Customers>();
            mapper.AddMapping(c => c.Id, "CustomerID");

            using (var dep = new SqlTableDependency<Customers>(connectionString, "Customers", mapper))
            {
                dep.OnChanged += Changed;
                dep.OnError += OnError;
                dep.Start();

                Console.WriteLine(@"Waiting for receiving notifications...");
                Console.WriteLine(@"Press a key to stop");
                Console.ReadKey();
            }
        }

        private static void OnError(object sender, ErrorEventArgs e)
        {
            Console.WriteLine(e.Error.Message);
        }

        private static void Changed(object sender, RecordChangedEventArgs<Customers> e)
        {
            Console.WriteLine(Environment.NewLine);

            if (e.ChangeType != ChangeType.None)
            {
                var changedEntity = e.Entity;
                Console.WriteLine(@"DML operation: " + e.ChangeType);
                Console.WriteLine(@"CompanyName: " + changedEntity.CompanyName);
                Console.WriteLine(@"ContactName: " + changedEntity.ContactName);
                Console.WriteLine(@"CustomerID: " + changedEntity.Id);
            }
        }
    }
}