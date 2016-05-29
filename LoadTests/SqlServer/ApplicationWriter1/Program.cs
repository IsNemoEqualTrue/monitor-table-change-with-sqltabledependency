using System;
using System.Configuration;
using System.Data.SqlClient;

namespace ApplicationWriter1
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

            Console.Title = new string('*', 10) + " SQL ServerDB Writer B " + new string('*', 10);

            System.Threading.Thread.Sleep(6000);


            var connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;

            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    while (index < total)
                    {
                        switch (new Random().Next(1, 4))
                        {
                            case 1:
                                sqlCommand.CommandText = $"UPDATE [Customers] SET [First Name] = 'HHH', [Second Name] = '" + Guid.NewGuid().ToString() + "' WHERE ID = 'B" + (insertedCnt - 1) + "'";
                                Console.WriteLine("Writer B executed: " + Environment.NewLine + sqlCommand.CommandText);
                                if (sqlCommand.ExecuteNonQuery() > 0) updatedCnt++;
                                break;
                            case 2:
                                sqlCommand.CommandText = $"INSERT INTO [Customers] ([Id], [First Name], [Second Name]) VALUES ('B{insertedCnt}', 'DDD', 'AAS')";
                                Console.WriteLine("Writer B executed: " + Environment.NewLine + sqlCommand.CommandText);
                                if (sqlCommand.ExecuteNonQuery() > 0) insertedCnt++;
                                break;
                            case 3:
                                sqlCommand.CommandText = "DELETE FROM [Customers] WHERE ID = 'B" + (insertedCnt - 1) + "'";
                                Console.WriteLine("Writer B executed: " + Environment.NewLine + sqlCommand.CommandText);
                                if (sqlCommand.ExecuteNonQuery() > 0) deletedCnt++;
                                break;
                        }

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