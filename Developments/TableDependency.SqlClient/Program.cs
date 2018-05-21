using System;
using System.Configuration;

using TableDependency.Enums;
using TableDependency.EventArgs;

using ErrorEventArgs = TableDependency.EventArgs.ErrorEventArgs;

namespace TableDependency.SqlClient.Development
{
    public class Program
    {
        private static void Main()
        {
            var connectionString = string.Empty;
            ConsoleKeyInfo consoleKeyInfo;

            do
            {
                Console.Clear();

                Console.WriteLine("TableDependency, SqlTableDependency");
                Console.WriteLine("Copyright (c) 2015-2018 Christian Del Bianco.");
                Console.WriteLine("All rights reserved." + Environment.NewLine);
                Console.WriteLine("**********************************************************************************************");
                Console.WriteLine("Choose connection string:");
                Console.WriteLine(" - F4: SQL Server Developer 2008 - (DESKTOP-DFTT9LE\\SQLSERVER2008) user sa");
                Console.WriteLine(" - F5: SQL Server Developer 2008 - (DESKTOP-DFTT9LE\\SQLSERVER2008) user Test_User");
                Console.WriteLine(" - ESC to exit");
                Console.WriteLine("**********************************************************************************************");

                consoleKeyInfo = Console.ReadKey();
                if (consoleKeyInfo.Key == ConsoleKey.Escape) Environment.Exit(0);

            } while (consoleKeyInfo.Key != ConsoleKey.F4 && consoleKeyInfo.Key != ConsoleKey.F5);
           
            if (consoleKeyInfo.Key == ConsoleKey.F4) connectionString = ConfigurationManager.ConnectionStrings["SqlServer2008 sa"].ConnectionString;
            if (consoleKeyInfo.Key == ConsoleKey.F5) connectionString = ConfigurationManager.ConnectionStrings["SqlServer2008 Test_User"].ConnectionString;

            var mapper = new ModelToTableMapper<Customer>();
            mapper.AddMapping(c => c.Id, "CustomerID");

            var updateOf = new UpdateOfModel<Customer>();
            updateOf.Add(i => i.CompanyName);
            updateOf.Add(i => i.ContactName);

            using (var dep = new SqlTableDependency<Customer>(connectionString, "Customers", mapper: mapper, updateOf: updateOf, includeOldValues: true))
            {
                dep.OnChanged += Changed;
                dep.OnError += OnError;
                dep.Start();

                Console.WriteLine();
                Console.WriteLine("Waiting for receiving notifications (db objects naming: " + dep.DataBaseObjectsNamingConvention + ")...");
                Console.WriteLine("Press a key to stop.");
                Console.ReadKey();
            }            
        }

        private static void OnError(object sender, ErrorEventArgs e)
        {
            Console.Clear();

            Console.WriteLine(e.Message);
            Console.WriteLine(e.Error?.Message);
        }

        private static void Changed(object sender, RecordChangedEventArgs<Customer> e)
        {
            Console.WriteLine(Environment.NewLine);

            if (e.ChangeType != ChangeType.None)
            {
                var changedEntity = e.Entity;
                Console.WriteLine("DML operation: " + e.ChangeType);
                Console.WriteLine("CompanyName:   " + changedEntity.CompanyName);
                Console.WriteLine("ContactName:   " + changedEntity.ContactName);
            }

            if (e.ChangeType == ChangeType.Update)
            {
                var changedEntity = e.EntityOldValues;
                Console.WriteLine("CompanyName (OLD):   " + changedEntity.CompanyName);
                Console.WriteLine("ContactName (OLD):   " + changedEntity.ContactName);
            }
        }
    }
}