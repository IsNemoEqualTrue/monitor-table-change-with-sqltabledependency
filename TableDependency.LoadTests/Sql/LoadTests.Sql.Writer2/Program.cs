using System;
using System.Data.SqlClient;

namespace LoadTests.Sql.Writer2
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Title = new string('*', 10) + " SQL Server DB Writer 2 " + new string('*', 10);

            System.Threading.Thread.Sleep(1 * 30 * 1000);

            string sql = null;

            using (var sqlConnection = new SqlConnection("data source=.;initial catalog=TableDependencyDB;integrated security=True"))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    while (true)
                    {
                        switch (new Random().Next(1, 3))
                        {
                            case 1:
                                sql = $"INSERT INTO [Customers] ([First Name], [Second Name]) VALUES ('{DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss.sss")}', '{"INSERT from Writer 2"}')";
                                break;
                            case 2:
                                sql = "DELETE FROM [Customers]";
                                break;
                            case 3:
                                sql = $"UPDATE [Customers] SET [First Name] = '{DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss.sss")}', [Second Name] = '{"UPDATE from Writer 2"}'";
                                break;
                        }

                        Console.WriteLine("Writer 2 executing: " + Environment.NewLine + sql);
                        sqlCommand.CommandText = sql;
                        sqlCommand.ExecuteNonQuery();
                        System.Threading.Thread.Sleep(2000);
                    }
                }
            }
        }
    }
}