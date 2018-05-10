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
            var connectionString = ConfigurationManager.ConnectionStrings["SqlServer2008 Test_User"].ConnectionString;
            var mapper = new ModelToTableMapper<Filter>();
            mapper.AddMapping(c => c.Surname, "Last Name");
            mapper.AddMapping(c => c.Id, "Identificator");

            Expression<Func<Filter, bool>> expression = p => p.Id == 2;
            ITableDependencyFilter filterExpression = new SqlTableDependencyFilter<Filter>(expression, mapper);

            using (var dep = new SqlTableDependency<Filter>(connectionString, "Filter", mapper: mapper, filter: filterExpression))
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

        private static void Changed(object sender, RecordChangedEventArgs<Filter> e)
        {
            Console.WriteLine(Environment.NewLine);

            if (e.ChangeType != ChangeType.None)
            {
                var changedEntity = e.Entity;
                Console.WriteLine(@"DML operation: " + e.ChangeType);
                Console.WriteLine(@"CustomerID:    " + changedEntity.Id);
                Console.WriteLine(@"CategoryId:    " + changedEntity.Name);
                Console.WriteLine(@"Name:          " + changedEntity.Surname);
            }
        }
    }
}