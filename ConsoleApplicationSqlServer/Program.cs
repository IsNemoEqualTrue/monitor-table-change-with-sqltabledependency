using System;
using System.Configuration;
using System.Data.SqlClient;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Mappers;
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
            //DropAndCreateTable(connectionString);

            var mapper = new ModelToTableMapper<_Guild>();
            mapper.AddMapping(c => c.id, "ID");

            using (var tableDependency = new SqlTableDependency<_Guild>(connectionString, "[_Guild]", mapper))
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

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<_Guild> e)
        {
            Console.WriteLine(Environment.NewLine);

            if (e.ChangeType != ChangeType.None)
            {
                var changedEntity = e.Entity;
                Console.WriteLine(@"DML operation: " + e.ChangeType);
                Console.WriteLine(@"id: " + changedEntity.id);
                Console.WriteLine(@"Name: " + changedEntity.Name);
                Console.WriteLine(@"GatheredSP: " + changedEntity.GatheredSP);
            }
        }
    }
}