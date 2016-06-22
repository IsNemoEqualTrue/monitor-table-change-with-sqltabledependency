using System;
using System.Configuration;
using Oracle.ManagedDataAccess.Client;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.OracleClient;
using Oracle.ManagedDataAccess.Types;

namespace ConsoleApplicationOracle
{
    //public class DataTimeModel
    //{
    //    // *****************************************************
    //    // Oracle Data Type Mappings:
    //    // https://msdn.microsoft.com/en-us/library/cc716726%28v=vs.110%29.aspx?f=255&MSPPError=-2147217396
    //    // *****************************************************
    //    public DateTime DateColum { get; set; }
    //    public DateTime TimeStampColumn { get; set; }
    //    public DateTimeOffset TimeStampWithTimeZone { get; set; }
    //    public DateTime TimeStampWithLocalTimeZone { get; set; }
        

    //    public TimeSpan IntervalDayToSecondColumn { get; set; }
    //    public Int64 IntervalYearToMonthColumn { get; set; }
    //}

    public class CurrentAlarmMonitor
    {
        public string ProvinceCode { get; set; }
        public int EventStatus { get; set; }
        public string Note { get; set; }
    }

    class Program
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;

        private const string TableName = "CURRENTALARMS";

        //internal static void DropAndCreateTable(string connectionString, string tableName)
        //{
        //    using (var connection = new OracleConnection(connectionString))
        //    {
        //        connection.Open();
        //        using (var command = connection.CreateCommand())
        //        {
        //            command.CommandText = $"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {tableName.ToUpper()}'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;";
        //            command.ExecuteNonQuery();
        //            command.CommandText = $"CREATE TABLE {tableName}(DATECOLUM DATE,TIMESTAMPCOLUMN TIMESTAMP(6),TIMESTAMPWITHTIMEZONE TIMESTAMP WITH TIME ZONE,TIMESTAMPWITHLOCALTIMEZONE TIMESTAMP WITH LOCAL TIME ZONE, INTERVALDAYTOSECONDCOLUMN INTERVAL DAY(2) TO SECOND(6),INTERVALYEARTOMONTHCOLUMN INTERVAL YEAR(2) TO MONTH)";
        //            command.ExecuteNonQuery();
        //        }
        //    }
        //}

        static void Main()
        {
            //DropAndCreateTable(ConnectionString, TableName);

            using (var tableDependency = new OracleTableDependency<CurrentAlarmMonitor>(ConnectionString, TableName))
            {
                tableDependency.OnChanged += Changed;
                tableDependency.OnStatusChanged += TableDependency_OnStatusChanged;
                tableDependency.OnError += tableDependency_OnError;

                tableDependency.Start();
                Console.WriteLine(@"Waiting for receiving notifications: change some records in the table...");
                Console.WriteLine(@"Press a key to exit");
                Console.ReadKey();
            }
        }

        private static void TableDependency_OnStatusChanged(object sender, StatusChangedEventArgs e)
        {
            Console.WriteLine(@"Status: " + e.Status);
        }

        static void tableDependency_OnError(object sender, ErrorEventArgs e)
        {
            Console.WriteLine("ERROR:");
            Console.WriteLine(e.Error.Message);
            Console.WriteLine(e.Error.StackTrace);
        }

        static void Changed(object sender, RecordChangedEventArgs<CurrentAlarmMonitor> e)
        {
            Console.WriteLine(Environment.NewLine);

            if (e.ChangeType != ChangeType.None)
            {
                Console.WriteLine(@"EventStatus: " + e.Entity.EventStatus);
                Console.WriteLine(@"Note: " + e.Entity.Note);
                Console.WriteLine(@"ProvinceCode: " + e.Entity.ProvinceCode);

                //var returnedDate = new OracleDate(e.Entity.DateColum);
                //Console.WriteLine(returnedDate.Value);
                //var timeStampReturned = new OracleTimeStamp(e.Entity.TimeStampColumn);
                //Console.WriteLine(timeStampReturned.Value.ToString("dd/MM/yyyy HH:mm:ss.ffffff"));
                //var timeStampWothTimeZoneReturned = e.Entity.TimeStampWithTimeZone;
                //Console.WriteLine(timeStampWothTimeZoneReturned.ToString("dd/MM/yyyy HH:mm:ss.ffffff zzz"));
                //var timeStampWothLocalTimeZoneReturned = e.Entity.TimeStampWithLocalTimeZone;
                //Console.WriteLine(timeStampWothLocalTimeZoneReturned.ToString("dd/MM/yyyy HH:mm:ss.ffffff"));
            }
        }
    }
}
