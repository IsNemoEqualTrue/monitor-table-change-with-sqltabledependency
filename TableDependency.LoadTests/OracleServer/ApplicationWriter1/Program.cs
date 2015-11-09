using System;
using Oracle.DataAccess.Client;

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
            Console.ForegroundColor = ConsoleColor.Red;
            Console.Title = new String('*', 10) + " ORACLE DB Writer 1 " + new String('*', 10);

            System.Threading.Thread.Sleep(5000);

            string sql = null;

            using (var sqlConnection = new OracleConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    while (true)
                    {
                        switch (new Random().Next(1, 3))
                        {
                            case 1:
                                sql = $"UPDATE PRODUCTS SET NAME = '{DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss")}', \"Long Description\" = '{"UPDATE from Writer 1"}'";
                                break;
                            case 2:
                                sqlCommand.CommandText = "SELECT SEQ_PRODUCTS.nextval FROM DUAL";
                                var id = Convert.ToInt32(sqlCommand.ExecuteScalar());
                                sql = String.Format("INSERT INTO PRODUCTS (ID, NAME, \"Long Description\") VALUES ({0}, '{1}', '{2}')", id, DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss"), "<en>INSERT from Writer 1<en>");
                                break;
                            case 3:
                                sql = String.Format("DELETE FROM PRODUCTS");
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