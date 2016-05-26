using System;
using System.Data.SqlClient;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient;

namespace TableDependency.IssueTests.Issues
{
    internal class Issue24Model
    {
        public string Id { get; set; }
        public string Message { get; set; }
    }

    internal class Issue27 : IIssue
    {
        private const string TableName = "Issue27Model";

        private static void DropAndCreateTable(string connectionString)
        {
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('[{TableName}]', 'U') IS NOT NULL DROP TABLE [dbo].[{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [int] NULL, [Message] [VARCHAR](100) NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        public void Test()
        {
            var connectionString = "data source=.;initial catalog=TableDependencyDB;integrated security=True";
            DropAndCreateTable(connectionString);

            using (var tableDependency = new SqlTableDependency<Issue24Model>(connectionString, TableName))
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

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Issue24Model> e)
        {
            Console.WriteLine(Environment.NewLine);

            if (e.ChangeType != ChangeType.None)
            {
                var changedEntity = e.Entity;
                Console.WriteLine("DML operation: " + e.ChangeType);
                Console.WriteLine("ID: " + changedEntity.Id);
                Console.WriteLine("Message: " + changedEntity.Message);
            }
        }
    }
}