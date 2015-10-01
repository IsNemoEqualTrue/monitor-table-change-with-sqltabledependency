namespace TableDependency.SqlClient.IntegrationTest.Helpers
{
    public static class BooleanExtender
    {
        public static string ToBit(this bool value)
        {
            return value ? "1" : "0";
        }

        public static string ToNullableBit(this bool? value)
        {
            if (value == null) return "NULL";
            return value.GetValueOrDefault() ? "1" : "0";
        }
    }
}