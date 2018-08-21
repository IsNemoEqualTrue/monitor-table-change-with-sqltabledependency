using System;
using System.Configuration;
using System.Data.SqlClient;

using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test.LoadTests.Listener
{
    internal class Program
    {
        private static int _insertCounter;
        private static int _updateCounter;
        private static int _deleteCounter;

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

            if (e.ChangeType == ChangeType.Insert) _insertCounter++;
            if (e.ChangeType == ChangeType.Update) _updateCounter++;
            if (e.ChangeType == ChangeType.Delete) _deleteCounter++;

            var changedEntity = e.Entity;
            Console.WriteLine("DML operation: " + e.ChangeType);
            Console.WriteLine("Id: " + changedEntity.Id);
            Console.WriteLine("FirstName: " + changedEntity.FirstName);
            Console.WriteLine("SecondName: " + changedEntity.SecondName);
            Console.WriteLine("---------------------------------------------");
            Console.WriteLine("Insert: " + _insertCounter);
            Console.WriteLine("Update: " + _updateCounter);
            Console.WriteLine("Delete: " + _deleteCounter);
        }
    }
}