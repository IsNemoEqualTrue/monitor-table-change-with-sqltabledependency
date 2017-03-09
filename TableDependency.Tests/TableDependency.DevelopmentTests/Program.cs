using System;
using System.Configuration;
using System.Linq.Expressions;
using TableDependency;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient;
using TableDependency.SqlClient.Where;
using ErrorEventArgs = TableDependency.EventArgs.ErrorEventArgs;

namespace ConsoleApplicationSqlServer
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

                Console.WriteLine(@"TableDependency, SqlTableDependency");
                Console.WriteLine(@"Copyright (c) 2015-2017 Christian Del Bianco.");
                Console.WriteLine(@"All rights reserved." + Environment.NewLine);
                Console.WriteLine(@"************************************************************");
                Console.WriteLine(@"Application used for development [connection string to use]:");
                Console.WriteLine(@" F1: Integrated security");
                Console.WriteLine(@" F2: SQL Server authentication using user with DB Owner Role");
                Console.WriteLine(@" F3: SQL Server authentication using user not DBO");
                Console.WriteLine(@" ESC to exit");
                Console.WriteLine(@"************************************************************");

                consoleKeyInfo = Console.ReadKey();
                if (consoleKeyInfo.Key == ConsoleKey.Escape) Environment.Exit(0);

            } while (consoleKeyInfo.Key != ConsoleKey.F1 && consoleKeyInfo.Key != ConsoleKey.F2 && consoleKeyInfo.Key != ConsoleKey.F3);

            
            if (consoleKeyInfo.Key == ConsoleKey.F1) connectionString = ConfigurationManager.ConnectionStrings["IntegratedSecurityConnectionString"].ConnectionString;
            if (consoleKeyInfo.Key == ConsoleKey.F2) connectionString = ConfigurationManager.ConnectionStrings["DbOwnerSqlServerConnectionString"].ConnectionString;
            if (consoleKeyInfo.Key == ConsoleKey.F3) connectionString = ConfigurationManager.ConnectionStrings["UserNotDboConnectionString"].ConnectionString;

            var mapper = new ModelToTableMapper<Customers>();
            mapper.AddMapping(c => c.Id, "CustomerID");

            //Expression<Func<Customers, bool>> expression = p => p.ContactName.Trim() == "123";
            //var filter = new SqlTableDependencyFilter(expression);

            using (var dep = new SqlTableDependency<Customers>(
                connectionString, 
                tableName: "Customers", 
                mapper: mapper))
            {
                dep.OnChanged += Changed;
                dep.OnError += OnError;
                dep.Start();

                Console.WriteLine();
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
                Console.WriteLine(@"CustomerID:    " + changedEntity.Id);
                Console.WriteLine(@"ContactTitle:  " + changedEntity.ContactTitle);
                Console.WriteLine(@"CompanyName:   " + changedEntity.CompanyName);
                Console.WriteLine(@"ContactName:   " + changedEntity.ContactName);
                Console.WriteLine(@"Address:       " + changedEntity.Address);
                Console.WriteLine(@"City:          " + changedEntity.City);
                Console.WriteLine(@"PostalCode:    " + changedEntity.PostalCode);
                Console.WriteLine(@"Country:       " + changedEntity.Country);
            }
        }
    }
}