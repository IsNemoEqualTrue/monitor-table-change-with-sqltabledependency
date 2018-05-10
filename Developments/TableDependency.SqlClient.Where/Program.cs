using System;
using System.Configuration;
using System.Linq.Expressions;

using TableDependency.Abstracts;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient.Where.Development.Models;

namespace TableDependency.SqlClient.Where.Development
{
    public class Program
    {
        private static void Main()
        {
            // Get Connection string
            var connectionString = ConfigurationManager.ConnectionStrings["SqlServer2008 Test_User"].ConnectionString;

            // Because our model has a property that does not match table column name, we need a mapper C# Model PROPERTY <--> Database Table Column Name
            var mapper = new ModelToTableMapper<Product>();
            mapper.AddMapping(c => c.ItemsInStock, "Quantity");

            // Define WHERE filter specifing the WHERE condition
            // We also pass the mapper defined above as last contructor's parameter
            Expression<Func<Product, bool>> expression = p => (p.CategoryId == (int)CategorysEnum.Food || p.CategoryId == (int)CategorysEnum.Drink) && p.ItemsInStock <= 10;
            ITableDependencyFilter whereCondition = new SqlTableDependencyFilter<Product>(expression, mapper);

            // Create SqlTableDependency and pass filter condition, as weel as mapper
            using (var dep = new SqlTableDependency<Product>(connectionString, "Products", mapper: mapper, filter: whereCondition))
            {
                dep.OnChanged += Changed;
                dep.OnError += OnError;
                dep.Start();

                Console.WriteLine("TableDependency, SqlTableDependency, SqlTableDependencyFilter");
                Console.WriteLine("Copyright (c) 2015-2018 Christian Del Bianco.");
                Console.WriteLine("All rights reserved." + Environment.NewLine);
                Console.WriteLine();
                Console.WriteLine("Waiting for receiving notifications...");
                Console.WriteLine("Press a key to stop");
                Console.ReadKey();
            }
        }

        private static void OnError(object sender, ErrorEventArgs e)
        {
            Console.WriteLine(e.Error.Message);
        }

        private static void Changed(object sender, RecordChangedEventArgs<Product> e)
        {
            Console.WriteLine(Environment.NewLine);

            if (e.ChangeType != ChangeType.None)
            {
                var changedEntity = e.Entity;
                Console.WriteLine(@"DML operation: " + e.ChangeType);
                Console.WriteLine(@"CustomerID:    " + changedEntity.Id);
                Console.WriteLine(@"CategoryId:    " + changedEntity.CategoryId);
                Console.WriteLine(@"Name:          " + changedEntity.Name);
                Console.WriteLine(@"Quantity:      " + changedEntity.ItemsInStock);
            }
        }
    }
}