using System;

namespace TableDependency.SqlClient.IntegrationTest.Model
{
    public class Issue_0006_Model
    {
        public long Id { get; set; }
        public Nullable<bool> ProcessedNullableWithDefault { get; set; }
        public Nullable<bool> ProcessedNullable { get; set; }
        public bool Processed { get; set; }
    }
}