using System;
using System.Configuration;
using System.Linq.Expressions;

using TableDependency.SqlClient.Base;
using TableDependency.SqlClient.Base.Abstracts;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;
using TableDependency.SqlClient.Test.Client.Framework.Models;
using TableDependency.SqlClient.Where;

namespace TableDependency.SqlClient.Test.Client.Framework
{
    public class Program
    {
        private static void Main()
        {
            var connectionString = string.Empty;
            ConsoleKeyInfo consoleKeyInfo;
            var originalForegroundColor = Console.ForegroundColor;

            do
            {
                Console.Clear();
                
                Console.Write("TableDependency, SqlTableDependency");
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine(" (.NET Framework)");
                Console.ForegroundColor = originalForegroundColor;
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

            // Define WHERE filter specifing the WHERE condition
            // We also pass the mapper defined above as last contructor's parameter
            Expression<Func<Product, bool>> expression = p => (p.CategoryId == (int)CategorysEnum.Food || p.CategoryId == (int)CategorysEnum.Drink) && p.Quantity <= 10;
            ITableDependencyFilter whereCondition = new SqlTableDependencyFilter<Product>(expression, mapper);

            using (var dep = new SqlTableDependency<Product>(connectionString, "Products", mapper: mapper, includeOldValues: true, filter: whereCondition))
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