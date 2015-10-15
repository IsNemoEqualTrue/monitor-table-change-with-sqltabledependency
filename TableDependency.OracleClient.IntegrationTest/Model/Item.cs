using System.ComponentModel.DataAnnotations.Schema;

namespace TableDependency.OracleClient.IntegrationTest.Model
{
    [Table("Item")]
    public class Item
    {
        public long Id { get; set; }
        public string Name { get; set; }
        [Column("Long Description")]
        public string Description { get; set; }
        public int qty { get; set; }
    }
}