using System;
using System.Data.SqlClient;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.SqlClient;
using TableDependency.SqlClient.IntegrationTest.Model;

namespace ConsoleApplication2
{
    class Program
    {
        private static string _connectionString = "data source=.;initial catalog=TableDependencyDB;integrated security=True";
        private const string TableName = "Check_MultiRecordsUpdate";
        

        static void Main(string[] args)
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText =
                        $"CREATE TABLE [{TableName}]( " +
                        "[Id][int] NOT NULL, " +
                        "[First Name] [nvarchar](50) NOT NULL, " +
                        "[Second Name] [nvarchar](50) NOT NULL, " +
                        "[Born] [datetime] NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }

            SqlTableDependency<Check_Model> tableDependency = null;
            string naming = null;

            try
            {
                var mapper = new ModelToTableMapper<Check_Model>();
                mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second Name");

                tableDependency = new SqlTableDependency<Check_Model>(_connectionString, TableName, mapper);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;
                Console.WriteLine("Un tasto per uscire");
                Console.ReadKey();
            }
            finally
            {
                tableDependency?.Dispose();
            }
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Check_Model> e)
        {
            Console.WriteLine(e.Entity.Name);
        }
    }
}
