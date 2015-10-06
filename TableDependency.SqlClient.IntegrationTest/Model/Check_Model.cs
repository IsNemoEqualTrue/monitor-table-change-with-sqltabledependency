using System;
using System.Data.Spatial;
using System.Xml;

namespace TableDependency.SqlClient.IntegrationTest.Model
{
    public class Check_Model
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
        public DateTime Born { get; set; }

        // *****************************************************
        // All columns tests
        // *****************************************************
        public string varcharMAXColumn { get; set; }
        public string nvarcharMAXColumn { get; set; }
        public byte[] varbinaryMAXColumn { get; set; }
        public string xmlColumn { get; set; }
        public DateTime? dateColumn { get; set; }
        public DateTime? datetimeColumn { get; set; }
        public DateTime? datetime2Column { get; set; }
        public DateTimeOffset? datetimeoffsetColumn { get; set; }
        public long? bigintColumn { get; set; }
        public decimal? decimal18Column { get; set; }
        public decimal? decimal54Column { get; set; }
        public float? floatColumn { get; set; }
        public byte[] binary50Column { get; set; }
        public bool? bitColumn { get; set; }
        public bool bit2Column { get; set; }
        public bool bit3Column { get; set; }
        public char[] char10Column { get; set; }
        public byte[] varbinary50Column { get; set; }
        public string varchar50Column { get; set; }
        public string nvarchar50Column { get; set; }
        public decimal numericColumn { get; set; }

        public Guid uniqueidentifierColumn { get; set; }
        public Nullable<TimeSpan> time7Column { get; set; }
        public byte tinyintColumn { get; set; }
        public DateTime smalldatetimeColumn { get; set; }
        public short smallintColumn { get; set; }
        public Decimal moneyColumn{ get; set; }
        public Decimal smallmoneyColumn { get; set; }

        // *****************************************************
        // Column not present in database table: will be ignored
        // *****************************************************
        public string Address { get; set; }
        public string City { get; set; }
        public int Zip { get; set; }
        public string Country { get; set; }
    }
}