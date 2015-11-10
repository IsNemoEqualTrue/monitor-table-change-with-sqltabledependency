using System;
using System.Data.SqlClient;

namespace LoadTests.Sql.Writer1
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Title = new String('*', 10) + " SQL ServerDB Writer 1 " + new String('*', 10);

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
                                sql = $"UPDATE [Customers] SET [First Name] = '{DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss.sss")}', [Second Name] = '{"UPDATE from Writer 1"}'";
                                break;
                            case 2:
                                sql = $"INSERT INTO [Customers] ([First Name], [Second Name]) VALUES ('{DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss.sss")}', '{"INSERT from Writer 1"}')";
                                break;
                            case 3:
                                sql = string.Format("DELETE FROM [Customers]");
                                break;
                        }

                        Console.WriteLine("Writer 1 executing: " + Environment.NewLine + sql);
                        sqlCommand.CommandText = sql;
                        sqlCommand.ExecuteNonQuery();
                        System.Threading.Thread.Sleep(1000);
                    }
                }
            }
        }
    }
}