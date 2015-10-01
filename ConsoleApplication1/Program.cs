using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TableDependency.EventArgs;
using TableDependency.SqlClient;

namespace ConsoleApplication1
{
    class Program
    {
        static string TableName = "MQMessage";

        static void Main(string[] args)
        {
            createtable();
            using (var sqlTableDependency = new SqlTableDependency<Issue_0006_Model>("data source=.;initial catalog=TableDependencyDB;integrated security=True", TableName))
            {
                sqlTableDependency.OnChanged += SqlTableDependency_OnChanged;
                sqlTableDependency.OnError += SqlTableDependency_OnError;
                sqlTableDependency.Start();

                Console.ReadKey();
            }
        }

        static void createtable()
        {
            using (var sqlConnection = new SqlConnection("data source=.;initial catalog=TableDependencyDB;integrated security=True"))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}]";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}](" +
                        "[Id] [INT] IDENTITY(1, 1) NOT NULL PRIMARY KEY, " +
                        "[ProcessedNullableWithDefault] [BIT] NULL DEFAULT 0," +
                        "[ProcessedNullable] [BIT] NULL," +
                        "[Processed] [BIT] NOT NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        private static void SqlTableDependency_OnError(object sender, TableDependency.EventArgs.ErrorEventArgs e)
        {
            throw e.Error;
        }

        private static void SqlTableDependency_OnChanged(object sender, RecordChangedEventArgs<Issue_0006_Model> e)
        {
            Console.WriteLine(@"{0} row", e.ChangeType);
            Console.WriteLine(@"{0}, {1}, {2}", e.Entity.ProcessedNullableWithDefault, e.Entity.ProcessedNullable, e.Entity.Processed);
        }
    }

    public partial class Issue_0006_Model
    {
        public long Id { get; set; }
        public Nullable<bool> ProcessedNullableWithDefault { get; set; }
        public Nullable<bool> ProcessedNullable { get; set; }
        public bool Processed { get; set; }
    }
}
