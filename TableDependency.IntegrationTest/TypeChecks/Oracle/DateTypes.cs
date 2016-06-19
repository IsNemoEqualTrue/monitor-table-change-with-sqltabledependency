using System;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.OracleClient;

namespace TableDependency.IntegrationTest.TypeChecks.Oracle
{
    public class DataTimeModel
    {
        // *****************************************************
        // Oracle Data Type Mappings:
        // https://msdn.microsoft.com/en-us/library/cc716726%28v=vs.110%29.aspx?f=255&MSPPError=-2147217396
        // *****************************************************
        public DateTime DateColum { get; set; }
        public DateTime TimeStampColumn { get; set; }
        public DateTimeOffset TimeStampWithTimeZone { get; set; }
        public DateTime TimeStampWithLocalTimeZone { get; set; }

        public TimeSpan IntervalDayToSecondColumn { get; set; }
        public long IntervalYearToMonthColumn { get; set; }
    }

    [TestClass]
    public class DateTypes
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static readonly string TableName = "DATETIMETESTS";

        private static DateTime dateColum;
        private static OracleTimeStamp timeStampColumn;
        private static DateTimeOffset timeStampWithTimeZone;
        private static OracleTimeStampLTZ timeStampWithLocalTimeZone;
        private static OracleIntervalDS intervalDayToSecondColumn;
        private static OracleIntervalYM intervalYearToMonthColumn;

        private static DateTime dateColumReturned;
        private static OracleTimeStamp timeStampColumnReturned;
        private static DateTimeOffset timeStampWithTimeZoneReturned;
        private static OracleTimeStampLTZ timeStampWithLocalTimeZoneReturned;
        private static OracleIntervalDS intervalDayToSecondColumnReturned;
        private static OracleIntervalYM intervalYearToMonthColumnReturned;

        public TestContext TestContext { get; set; }

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            OracleHelper.DropTable(ConnectionString, TableName);

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {TableName}(DATECOLUM DATE,TIMESTAMPCOLUMN TIMESTAMP(6),TIMESTAMPWITHTIMEZONE TIMESTAMP WITH TIME ZONE,TIMESTAMPWITHLOCALTIMEZONE TIMESTAMP WITH LOCAL TIME ZONE, INTERVALDAYTOSECONDCOLUMN INTERVAL DAY(2) TO SECOND(6),INTERVALYEARTOMONTHCOLUMN INTERVAL YEAR(2) TO MONTH)";
                    command.ExecuteNonQuery();
                }
            }

            dateColum = DateTime.Now;
            timeStampColumn = new OracleTimeStamp(DateTime.Now);
            timeStampWithLocalTimeZone = OracleTimeStampLTZ.GetSysDate();
            timeStampWithTimeZone = DateTimeOffset.Now;
            intervalDayToSecondColumn = new OracleIntervalDS(1);
            intervalYearToMonthColumn = new OracleIntervalYM(2);
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            OracleHelper.DropTable(ConnectionString, TableName);
        }


        [TestCategory("Oracle")]
        [TestMethod]
        public void CheckDateTypeTest()
        {
            OracleTableDependency<DataTimeModel> tableDependency = null;

            try
            {
                tableDependency = new OracleTableDependency<DataTimeModel>(ConnectionString, TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.Start();             
                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(Truncate(timeStampColumn.Value, TimeSpan.FromSeconds(2)), Truncate(timeStampColumnReturned.Value, TimeSpan.FromSeconds(2)));
            Assert.AreEqual(dateColum.Date, dateColumReturned.Date);           
            Assert.AreEqual(Truncate(timeStampWithTimeZone.DateTime, TimeSpan.FromMilliseconds(1)), Truncate(timeStampWithTimeZoneReturned.DateTime, TimeSpan.FromMilliseconds(1)));
            Assert.AreEqual(Truncate(timeStampWithLocalTimeZone.Value, TimeSpan.FromMilliseconds(1)), Truncate(timeStampWithLocalTimeZoneReturned.Value, TimeSpan.FromMilliseconds(1)));
            Assert.AreEqual(intervalDayToSecondColumn.Value, intervalDayToSecondColumnReturned.Value);
            Assert.AreEqual(intervalYearToMonthColumn.Value, intervalYearToMonthColumnReturned.Value);
        }

        public static DateTime Truncate(DateTime dateTime, TimeSpan timeSpan)
        {
            if (timeSpan == TimeSpan.Zero) return dateTime; // Or could throw an ArgumentException
            return dateTime.AddTicks(-(dateTime.Ticks % timeSpan.Ticks));
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<DataTimeModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    dateColumReturned = e.Entity.DateColum;
                    timeStampColumnReturned = new OracleTimeStamp(e.Entity.TimeStampColumn);
                    timeStampWithTimeZoneReturned = e.Entity.TimeStampWithTimeZone;
                    timeStampWithLocalTimeZoneReturned = new OracleTimeStampLTZ(e.Entity.TimeStampWithLocalTimeZone);
                    intervalDayToSecondColumnReturned = new OracleIntervalDS(e.Entity.IntervalDayToSecondColumn);
                    intervalYearToMonthColumnReturned = new OracleIntervalYM(e.Entity.IntervalYearToMonthColumn);
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            using (var connection = new OracleConnection(ConnectionString))
            {              
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $"BEGIN INSERT INTO {TableName}(DATECOLUM, TIMESTAMPCOLUMN, TIMESTAMPWITHTIMEZONE, TIMESTAMPWITHLOCALTIMEZONE, INTERVALDAYTOSECONDCOLUMN, INTERVALYEARTOMONTHCOLUMN) " +
                        $"VALUES (:dateColumn, :timeStampColumn, :timeStampWithTimeZone, :timeStampWithLocalTimeZone, :intervalDayToSecondColumn, :intervalYearToMonthColumn); END;";

                    command.Parameters.Add(new OracleParameter(":dateColumn", dateColum));
                    command.Parameters.Add(new OracleParameter(":timeStampColumn", timeStampColumn));
                    command.Parameters.Add(new OracleParameter(":timeStampWithTimeZone", timeStampWithTimeZone.DateTime));
                    command.Parameters.Add(new OracleParameter(":timeStampWithLocalTimeZone", timeStampWithLocalTimeZone));
                    command.Parameters.Add(new OracleParameter(":intervalDayToSecondColumn", intervalDayToSecondColumn));
                    command.Parameters.Add(new OracleParameter(":intervalYearToMonthColumn", intervalYearToMonthColumn));
                    command.ExecuteNonQuery();
                }

                Thread.Sleep(5000);
            }
        }
    }
}