using System;

namespace TableDependency.SqlClient.IntegrationTest.Model
{
    public class Issue_0004_Model
    {
        public int Id { get; set; }
        public string VarcharColumn { get; set; }
        public decimal DecimalColumn { get; set; }
        public float FloatColumn { get; set; }
        public long NumericColumn { get; set; }
        public string CharColumn { get; set; }
        public DateTime DateTime2Column { get; set; }
        public DateTimeOffset DatetimeOffsetColumn { get; set; }
        public DateTime TimeColumn { get; set; }
    }
}