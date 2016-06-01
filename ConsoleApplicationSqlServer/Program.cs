using System;
using System.Configuration;
using System.Data.SqlClient;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient;

namespace ConsoleApplicationSqlServer
{
    class Program
    {
        private static void DropAndCreateTable(string connectionString)
        {
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = "IF OBJECT_ID('[Customers]', 'U') IS NOT NULL DROP TABLE [dbo].[Customers]";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = "CREATE TABLE [Customers]([Id] [VARCHAR](10) NOT NULL, [BirthDay] datetime NULL, [Salary] [float] NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        private static void Main()
        {
            var connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
            DropAndCreateTable(connectionString);

            using (var tableDependency = new SqlTableDependency<Customer>(connectionString, "[Customers]"))
            {
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.OnError += TableDependency_OnError;

                tableDependency.Start();
                Console.WriteLine(@"Waiting for receiving notifications...");
                Console.WriteLine(@"Press a key to stop");
                Console.ReadKey();
                tableDependency.Stop();
            }
        }

        private static void TableDependency_OnError(object sender, ErrorEventArgs e)
        {
            Console.WriteLine(e.Error.Message);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Customer> e)
        {
            Console.WriteLine(Environment.NewLine);

            if (e.ChangeType != ChangeType.None)
            {
                var changedEntity = e.Entity;
                Console.WriteLine(@"DML operation: " + e.ChangeType);
                Console.WriteLine(@"ID: " + changedEntity.Id);
                Console.WriteLine(@"Name: " + changedEntity.Name);
                Console.WriteLine(@"Surname: " + changedEntity.Surname);
                Console.WriteLine(@"BirthDay: " + changedEntity.BirthDay);
                Console.WriteLine(@"Salary: " + changedEntity.Salary);
            }
        }
    }
}