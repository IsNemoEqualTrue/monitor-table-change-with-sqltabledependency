using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TableDependency.Enums;
using TableDependency.EventArgs;

using TableDependency.Mappers;
using TableDependency.SqlClient;

namespace ConsoleApplication1
{
    public class Check_Model
    {
        // *****************************************************
        // Generic tests
        // *****************************************************
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
        public DateTime Born { get; set; }
        public int qty { get; set; }

        // *****************************************************
        // SQL Server Data Type Mappings: 
        // https://msdn.microsoft.com/en-us/library/cc716729%28v=vs.110%29.aspx?f=255&MSPPError=-2147217396
        // *****************************************************
        public string varcharMAXColumn { get; set; }
        public string nvarcharMAXColumn { get; set; }
        public byte[] varbinaryMAXColumn { get; set; }
        public string xmlColumn { get; set; }
        public DateTime? dateColumn { get; set; }
        public DateTime? datetimeColumn { get; set; }
        public DateTime? datetime2Column { get; set; }
        public DateTimeOffset? datetimeoffsetColumn { get; set; }
        public long? bigintColumn { get; set; }
        public decimal? decimal18Column { get; set; }
        public decimal? decimal54Column { get; set; }
        public float? floatColumn { get; set; }
        public byte[] binary50Column { get; set; }
        public bool? bitColumn { get; set; }
        public bool bit2Column { get; set; }
        public bool bit3Column { get; set; }
        public char[] char10Column { get; set; }
        public byte[] varbinary50Column { get; set; }
        public string varchar50Column { get; set; }
        public string nvarchar50Column { get; set; }
        public decimal numericColumn { get; set; }

        // *****************************************************
        // Oracle Data Type Mappings:
        // https://msdn.microsoft.com/en-us/library/cc716726%28v=vs.110%29.aspx?f=255&MSPPError=-2147217396
        // *****************************************************
        public DateTime DateColum { get; set; }
        public DateTime TimeStampColumn { get; set; }
        public DateTime TimeStampWithLocalTimeZone { get; set; }
        public DateTime TimeStampWithTimeZone { get; set; }
        public TimeSpan IntervalDayToSecondColumn { get; set; }
        public Int64 IntervalYearToMonthColumn { get; set; }




        // *****************************************************
        // Column not present in database table: will be ignored
        // *****************************************************
        public string Address { get; set; }
        public string City { get; set; }
        public int Zip { get; set; }
        public string Country { get; set; }
    }
    class Program
    {
        static void Main(string[] args)
        {
            using(var sqlConnection = new SqlConnection("data source=.;initial catalog=TableDependencyDB;integrated security=True"))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('AAA', 'U') IS NOT NULL DROP TABLE [AAA];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText =
                        $"CREATE TABLE [AAA]( " +
                        "[Id][int] IDENTITY(1, 1) NOT NULL, " +
                        "[First Name] [NVARCHAR](50) NOT NULL, " +
                        "[Second Name] [NVARCHAR](50) NOT NULL, " +
                        "[Born] [DATETIME] NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }





            
            SqlTableDependency<Check_Model> _tableDependency = null;
            try
            {
                var mapper = new ModelToTableMapper<Check_Model>();
                mapper.AddMapping(c => c.Name, "FIRST name");
                mapper.AddMapping(c => c.Surname, "Second Name");

                _tableDependency = new SqlTableDependency<Check_Model>("data source=.;initial catalog=TableDependencyDB;integrated security=True", "AAA", mapper);
                _tableDependency.OnChanged += TableDependency_Changed;
                _tableDependency.OnError += _tableDependency_OnError;


                _tableDependency.Start();

                Console.WriteLine("un tasto per uscire");
                Console.ReadKey();

                _tableDependency.Stop();
                
            }
            finally
            {
                _tableDependency?.Dispose();
            }
        }

        private static void _tableDependency_OnError(object sender, ErrorEventArgs e)
        {
            Console.WriteLine(e.Error.Message);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Check_Model> e)
        {

            Console.WriteLine(e.Entity.Name);
        }
    }
}
