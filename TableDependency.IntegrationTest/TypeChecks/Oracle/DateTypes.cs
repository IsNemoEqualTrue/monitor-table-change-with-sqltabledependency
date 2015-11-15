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
        public DateTime TimeStampWithLocalTimeZone { get; set; }

        public TimeSpan IntervalDayToSecondColumn { get; set; }
        public Int64 IntervalYearToMonthColumn { get; set; }
    }

    [TestClass]
    public class DateTypes
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static readonly string TableName = "DATETIMETESTS";

        private static OracleDate dateColum;
        private static OracleTimeStamp timeStampColumn;
        private static OracleTimeStampLTZ timeStampWithLocalTimeZone;
        private static OracleIntervalDS intervalDayToSecondColumn;
        private static OracleIntervalYM intervalYearToMonthColumn;

        private static OracleDate dateColumReturned;
        private static OracleTimeStamp timeStampColumnReturned;
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
                    command.CommandText = $"CREATE TABLE {TableName}(DATECOLUM DATE,TIMESTAMPCOLUMN TIMESTAMP(6),TIMESTAMPWITHLOCALTIMEZONE TIMESTAMP WITH LOCAL TIME ZONE,TIMESTAMPWITHTIMEZONE TIMESTAMP WITH TIME ZONE,INTERVALDAYTOSECONDCOLUMN INTERVAL DAY(2) TO SECOND(6),INTERVALYEARTOMONTHCOLUMN INTERVAL YEAR(2) TO MONTH)";
                    command.ExecuteNonQuery();
                }
            }

            dateColum = new OracleDate(DateTime.Now);
            timeStampColumn = DateTime.Now;
            timeStampWithLocalTimeZone = OracleTimeStampLTZ.GetSysDate();
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
                Thread.Sleep(1000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(2000);
            }
            finally
            {
                tableDependency?.Dispose();
            }


            Assert.AreEqual(dateColum.Value.Date, dateColumReturned.Value.Date);
            Assert.AreEqual(Truncate(timeStampColumn.Value, TimeSpan.FromSeconds(2)), Truncate(timeStampColumnReturned.Value, TimeSpan.FromSeconds(2)));
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
                    dateColumReturned = new OracleDate(e.Entity.DateColum);
                    timeStampColumnReturned = new OracleTimeStamp(e.Entity.TimeStampColumn);
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
                        $"BEGIN INSERT INTO {TableName}(DATECOLUM, TIMESTAMPCOLUMN, TIMESTAMPWITHLOCALTIMEZONE, TIMESTAMPWITHTIMEZONE, INTERVALDAYTOSECONDCOLUMN, INTERVALYEARTOMONTHCOLUMN) " + 
                        $"VALUES (:dateColumn, :timeStampColumn, :timeStampWithLocalTimeZone, :timeStampWithTimeZone, :intervalDayToSecondColumn, :intervalYearToMonthColumn); END;";

                    command.Parameters.Add(new OracleParameter("dateColumn", OracleDbType.Date) { Value = dateColum.Value.Date });
                    command.Parameters.Add(new OracleParameter("timeStampColumn", OracleDbType.TimeStamp) { Value = timeStampColumn });
                    command.Parameters.Add(new OracleParameter("timeStampWithLocalTimeZone", OracleDbType.TimeStampLTZ) { Value = timeStampWithLocalTimeZone });
                    command.Parameters.Add(new OracleParameter("intervalDayToSecondColumn", OracleDbType.IntervalDS) { Value = intervalDayToSecondColumn });
                    command.Parameters.Add(new OracleParameter("intervalYearToMonthColumn", OracleDbType.IntervalYM) { Value = intervalYearToMonthColumn });
                    command.ExecuteNonQuery();
                }

                Thread.Sleep(5000);
            }
        }
    }
}