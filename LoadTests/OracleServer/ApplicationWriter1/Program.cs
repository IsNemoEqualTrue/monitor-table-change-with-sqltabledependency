using System;
using Oracle.ManagedDataAccess.Client;

namespace ApplicationWriter1
{
    class Program
    {
        private const string ConnectionString = "Data Source = " +
                                "(DESCRIPTION = " +
                                " (ADDRESS_LIST = " +
                                " (ADDRESS = (PROTOCOL = TCP)" +
                                " (HOST = 127.0.0.1) " +
                                " (PORT = 1521) " +
                                " )" +
                                " )" +
                                " (CONNECT_DATA = " +
                                " (SERVICE_NAME = XE)" +
                                " )" +
                                ");" +
                                "User Id=SYSTEM;" +
                                "password=tiger;";

        static void Main(string[] args)
        {
            int deletedCnt = 0;
            int insertedCnt = 0;
            int updatedCnt = 0;
            int total = 10000;
            int index = 1;

            Console.Title = new string('*', 10) + " ORACLE DB Writer A " + new string('*', 10);

            System.Threading.Thread.Sleep(5000);

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    while (index < total)
                    {
                        switch (new Random().Next(1, 4))
                        {
                            case 1:
                                command.CommandText = $"INSERT INTO PRODUCTS (ID, NAME, DESCRIPTION) VALUES ('A{insertedCnt}', 'AAAASASA', 'AAASAWQSA')";
                                Console.WriteLine("Writer A executed: " + Environment.NewLine + command.CommandText);
                                if (command.ExecuteNonQuery() > 0) insertedCnt++;
                                break;
                            case 2:
                                command.CommandText = "DELETE FROM PRODUCTS WHERE ID = 'A" + (insertedCnt - 1) + "'";
                                Console.WriteLine("Writer A executed: " + Environment.NewLine + command.CommandText);
                                if (command.ExecuteNonQuery() > 0) deletedCnt++;
                                break;
                            case 3:
                                command.CommandText = "UPDATE PRODUCTS SET NAME = 'AAA', DESCRIPTION = 'AAAAS' WHERE ID = 'A" + (insertedCnt - 1) + "'";
                                Console.WriteLine("Writer A executed: " + Environment.NewLine + command.CommandText);
                                if (command.ExecuteNonQuery() > 0) updatedCnt++;
                                break;
                        }
                      
                        System.Threading.Thread.Sleep(1500);
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