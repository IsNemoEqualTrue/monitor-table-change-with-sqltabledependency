using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Oracle.DataAccess.Client;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.OracleClient;

namespace ConsoleApplication2
{
    public class AModel1
    {
        public DateTime DateColum { get; set; }
        public DateTime TimeStampColumn { get; set; }
        public DateTime TimeStampWithLocalTimeZone { get; set; }
        public DateTime TimeStampWithTimeZone { get; set; }
        public TimeSpan IntervalDayToSecondColumn { get; set; }
        public Int64 IntervalYearToMonthColumn { get; set; }

    }

    internal class Program
    {
        static string TableName = "AModel1";
        static string ConnectionString = "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=127.0.0.1)(PORT= 1521)))(CONNECT_DATA=(SERVICE_NAME = XE)));User Id=SYSTEM;password=tiger;";

        private static void Main(string[] args)
        {

            OracleHelper.DropTable(ConnectionString, TableName);

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {TableName}(DATECOLUM DATE,TIMESTAMPCOLUMN TIMESTAMP(6),TIMESTAMPWITHLOCALTIMEZONE TIMESTAMP WITH LOCAL TIME ZONE,TIMESTAMPWITHTIMEZONE TIMESTAMP WITH TIME ZONE,INTERVALDAYTOSECONDCOLUMN INTERVAL DAY(2) TO SECOND(6),INTERVALYEARTOMONTHCOLUMN INTERVAL YEAR(2) TO MONTH)";
                    command.ExecuteNonQuery();
                }
            }


            OracleTableDependency<AModel1> tableDependency = null;
            string naming = null;

            try
            {

                tableDependency = new OracleTableDependency<AModel1>(ConnectionString, TableName);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.OnError += TableDependency_OnError;


                tableDependency.Start();

                Console.WriteLine("un tasto per uscire");
                Console.ReadKey();

                tableDependency.Stop();

            }
            finally
            {
                tableDependency?.Dispose();
            }
        }

        private static void TableDependency_OnError(object sender, ErrorEventArgs e)
        {
            throw e.Error;
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<AModel1> e)
        {
            Console.WriteLine(e.Entity.DateColum);
           
        }
    }
}