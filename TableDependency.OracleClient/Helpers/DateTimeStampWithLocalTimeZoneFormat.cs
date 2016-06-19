namespace TableDependency.OracleClient.Helpers
{
    // The TIMESTAMP WITH LOCAL TIME ZONE datatype stores the timestamp without time zone information. 
    // It normalizes the data to the database time zone every time the data is sent to and from a client. It requires 11 bytes of storage.
    internal class DateTimeStampWithLocalTimeZoneFormat
    {
        public string OracleFormat => "dd/mm/yyyy hh24:mi:ss.FF";
        public string NetFormat => "dd/MM/yyyy HH:mm:ss.ffffff";
    }
}