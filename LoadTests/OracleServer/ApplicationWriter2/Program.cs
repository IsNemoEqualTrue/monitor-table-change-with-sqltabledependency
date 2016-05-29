using System;
using Oracle.ManagedDataAccess.Client;

namespace ApplicationWriter2
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

            Console.Title = new string('*', 10) + " ORACLE DB Writer B " + new string('*', 10);

            System.Threading.Thread.Sleep(5000);

            string sql = null;

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
                                command.CommandText = $"INSERT INTO PRODUCTS (ID, NAME, DESCRIPTION) VALUES ('B{insertedCnt}', 'BABBO', 'BBB')";
                                Console.WriteLine("Writer B executed: " + Environment.NewLine + command.CommandText);
                                if (command.ExecuteNonQuery() > 0) insertedCnt++;
                                break;
                            case 2:
                                command.CommandText = "UPDATE PRODUCTS SET NAME = 'BUBU', DESCRIPTION = 'BABA' WHERE ID = 'B" + (insertedCnt - 1) + "'";
                                Console.WriteLine("Writer B executed: " + Environment.NewLine + command.CommandText);
                                if (command.ExecuteNonQuery() > 0) updatedCnt++;
                                break;
                            case 3:
                                command.CommandText = "DELETE FROM PRODUCTS WHERE ID = 'B" + (insertedCnt - 1) + "'";
                                Console.WriteLine("Writer B executed: " + Environment.NewLine + command.CommandText);
                                if (command.ExecuteNonQuery() > 0) deletedCnt++;
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