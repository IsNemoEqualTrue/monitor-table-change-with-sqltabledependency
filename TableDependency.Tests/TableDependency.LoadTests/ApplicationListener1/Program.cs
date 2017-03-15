using System;
using System.Configuration;
using System.Data.SqlClient;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient;

namespace ApplicationListener1
{
    internal class Program
    {
        private static int insertCounter = 0;
        private static int updateCounter = 0;
        private static int deleteCounter = 0;

        private static void DropAndCreateTable(string connectionString)
        {
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = "IF OBJECT_ID('[LoadTest]', 'U') IS NOT NULL DROP TABLE [dbo].[LoadTest]";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = "CREATE TABLE [LoadTest] ([Id] [int], [FirstName] nvarchar(50), [SecondName] nvarchar(50))";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        private static void Main()
        {
            var connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
            DropAndCreateTable(connectionString);

            using (var tableDependency = new SqlTableDependency<LoadTest>(connectionString))
            {
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.OnError += TableDependency_OnError;

                tableDependency.Start();
                Console.WriteLine("Waiting for receiving notifications...");
                Console.WriteLine("Press a key to stop");
                Console.ReadKey();
                tableDependency.Stop();
            }
        }

        private static void TableDependency_OnError(object sender, ErrorEventArgs e)
        {
            Console.WriteLine(e.Error.Message);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<LoadTest> e)
        {
            Console.WriteLine(Environment.NewLine);

            if (e.ChangeType == ChangeType.Insert) insertCounter++;
            if (e.ChangeType == ChangeType.Update) updateCounter++;
            if (e.ChangeType == ChangeType.Delete) deleteCounter++;

            var changedEntity = e.Entity;
            Console.WriteLine("DML operation: " + e.ChangeType);
            Console.WriteLine("Id: " + changedEntity.Id);
            Console.WriteLine("FirstName: " + changedEntity.FirstName);
            Console.WriteLine("SecondName: " + changedEntity.SecondName);
            Console.WriteLine("---------------------------------------------");
            Console.WriteLine("Insert: " + insertCounter);
            Console.WriteLine("Update: " + updateCounter);
            Console.WriteLine("Delete: " + deleteCounter);
        }
    }
}