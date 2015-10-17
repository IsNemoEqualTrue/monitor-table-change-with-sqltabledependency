using System.ComponentModel.DataAnnotations.Schema;

namespace TableDependency.IntegrationTest.Models
{
    [Table(TableName)]
    public class Item4
    {
        public long Id { get; set; }

        public string Name { get; set; }

        [Column(ColumnName)]
        public string Description { get; set; }



        private const string ColumnName = "Long Description";
        public static string GetColumnName => ColumnName;

        private const string TableName = "ItemsTable";
        public static string GetTableName => TableName;
    }
}