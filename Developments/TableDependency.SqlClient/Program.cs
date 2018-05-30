using System;
using System.Configuration;

using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient.Development.Models;

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

                Console.ForegroundColor = ConsoleColor.Yellow;
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

            Console.ResetColor();
            if (consoleKeyInfo.Key == ConsoleKey.F4) connectionString = ConfigurationManager.ConnectionStrings["SqlServer2008 sa"].ConnectionString;
            if (consoleKeyInfo.Key == ConsoleKey.F5) connectionString = ConfigurationManager.ConnectionStrings["SqlServer2008 Test_User"].ConnectionString;

            var mapper = new ModelToTableMapper<Product>();
            mapper.AddMapping(c => c.Expiring, "ExpiringDate");

            using (var dep = new SqlTableDependency<Product>(connectionString, "Products", mapper: mapper, includeOldValues: true))
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

        private static void Changed(object sender, RecordChangedEventArgs<Product> e)
        {
            Console.WriteLine(Environment.NewLine);

            if (e.ChangeType != ChangeType.None)
            {
                var changedEntity = e.Entity;
                Console.WriteLine("Id: " + changedEntity.Id);
                Console.WriteLine("Name: " + changedEntity.Name);
                Console.WriteLine("Expiring: " + changedEntity.Expiring);
                Console.WriteLine("Quantity: " + changedEntity.Quantity);
                Console.WriteLine("Price: " + changedEntity.Price);
            }

            if (e.ChangeType == ChangeType.Update && e.EntityOldValues != null)
            {
                Console.WriteLine(Environment.NewLine);

                var changedEntity = e.EntityOldValues;
                Console.WriteLine("Id (OLD): " + changedEntity.Id);
                Console.WriteLine("Name (OLD): " + changedEntity.Name);
                Console.WriteLine("Expiring (OLD): " + changedEntity.Expiring);
                Console.WriteLine("Quantity (OLD): " + changedEntity.Quantity);
                Console.WriteLine("Price (OLD): " + changedEntity.Price);
            }
        }
    }
}