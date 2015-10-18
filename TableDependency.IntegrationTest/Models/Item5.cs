using System.ComponentModel.DataAnnotations.Schema;

namespace TableDependency.IntegrationTest.Models
{
    [Table("ItemsTable")]
    public class Item5
    {
        public long Id { get; set; }

        public string Name { get; set; }

        [Column("Long Description")]
        public string Infos { get; set; }
    }
}