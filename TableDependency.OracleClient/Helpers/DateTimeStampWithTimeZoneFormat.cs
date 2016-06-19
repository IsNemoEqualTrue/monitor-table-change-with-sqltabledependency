namespace TableDependency.OracleClient.Helpers
{
    internal class DateTimeStampWithTimeZoneFormat
    {
        public string OracleFormat => "dd/mm/yyyy hh24:mi:ss.FF TZH:TZM";
        public string NetTimeZoneFormat => "zzz";
        public string NetFormat => "dd/MM/yyyy HH:mm:ss.ffffff" + " " + this.NetTimeZoneFormat;
    }
}