using System;

namespace TableDependency.SqlClient.IntegrationTest.Model
{
    public class Issue_0008_Model
    {
        public float? floatColumn { get; set; }
        public decimal? decimal54Column { get; set; }
        public DateTime? dateColumn { get; set; }
        public DateTime? datetimeColumn { get; set; }
        public DateTime? datetime2Column { get; set; }
        public DateTimeOffset? datetimeoffsetColumn { get; set; }
    }
}