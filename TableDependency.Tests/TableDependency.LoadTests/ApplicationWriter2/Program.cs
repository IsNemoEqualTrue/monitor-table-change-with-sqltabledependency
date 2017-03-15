using System;
using System.Configuration;
using System.Data.SqlClient;

namespace ApplicationWriter2
{
    class Program
    {
        static void Main(string[] args)
        {
            int deletedCnt = 0;
            int insertedCnt = 0;
            int updatedCnt = 0;
            int total = 10000;
            int index = 1;
            int i = 1;

            Console.Title = new string('*', 10) + " SQL Server DB Writer 2 " + new string('*', 10);
            System.Threading.Thread.Sleep(6000);
            var connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    while (index < total)
                    {
                        switch (i)
                        {
                            case 1:
                                sqlCommand.CommandText = "INSERT INTO [LoadTest] ([Id], [FirstName], [SecondName]) VALUES (2, 'AAAA', 'AAAAS')";                                
                                if (sqlCommand.ExecuteNonQuery() > 0) insertedCnt++;
                                i++;
                                break;

                            case 2:
                                sqlCommand.CommandText = "UPDATE [LoadTest] SET [FirstName] = 'Guai grossi', [SecondName] = '" + Guid.NewGuid() + "' WHERE [Id] = 2";
                                if (sqlCommand.ExecuteNonQuery() > 0) updatedCnt++;
                                i++;
                                break;

                            case 3:
                                sqlCommand.CommandText = "DELETE FROM [LoadTest] WHERE [Id] = 2";
                                if (sqlCommand.ExecuteNonQuery() > 0) deletedCnt++;
                                i = 1;
                                break;
                        }

                        Console.WriteLine("Writer 2 executed: " + Environment.NewLine + sqlCommand.CommandText);
                        System.Threading.Thread.Sleep(50);
                        index++;
                    }
                }
            }

            Console.WriteLine("INSERT counter: " + insertedCnt);
            Console.WriteLine("UPDATE counter: " + updatedCnt);
            Console.WriteLine("DELETE counter: " + deletedCnt);
            Console.WriteLine(Environment.NewLine + "Press a key to exit");
            Console.ReadKey();
        }
    }
}