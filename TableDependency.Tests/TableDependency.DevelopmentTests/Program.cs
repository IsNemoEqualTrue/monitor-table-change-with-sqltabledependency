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
            var connectionString = string.Empty;
            ConsoleKeyInfo consoleKeyInfo;

            do
            {
                Console.Clear();

                Console.WriteLine(@"**************************************************");
                Console.WriteLine(@"Please select a connection code:");
                Console.WriteLine(@" F1: DBO with Integrated security=SSPI ");
                Console.WriteLine(@" F2: DB Owner Role");
                Console.WriteLine(@" F3: not DBO");
                Console.WriteLine(@" ESC:Exit");
                Console.WriteLine(@"**************************************************");

                consoleKeyInfo = Console.ReadKey();
                if (consoleKeyInfo.Key == ConsoleKey.Escape) Environment.Exit(0);

            } while (consoleKeyInfo.Key != ConsoleKey.F1 && consoleKeyInfo.Key != ConsoleKey.F2 && consoleKeyInfo.Key != ConsoleKey.F3);

            
            if (consoleKeyInfo.Key != ConsoleKey.F1) connectionString = ConfigurationManager.ConnectionStrings["DboWithIntegratedSecurityConnectionString"].ConnectionString;
            if (consoleKeyInfo.Key != ConsoleKey.F2) connectionString = ConfigurationManager.ConnectionStrings["DbOwnerSqlServerConnectionString"].ConnectionString;
            if (consoleKeyInfo.Key != ConsoleKey.F3) connectionString = ConfigurationManager.ConnectionStrings["UserNotDboConnectionString"].ConnectionString;

            var mapper = new ModelToTableMapper<Customers>();
            mapper.AddMapping(c => c.Id, "CustomerID");

            using (var dep = new SqlTableDependency<Customers>(connectionString, "Customers", mapper))
            {
                dep.OnChanged += Changed;
                dep.OnError += OnError;
                dep.Start();

                Console.WriteLine(Environment.NewLine);
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