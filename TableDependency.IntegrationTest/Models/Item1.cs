using System.ComponentModel.DataAnnotations.Schema;

namespace TableDependency.IntegrationTest.Models
{
    public class Item1
    {
        public long Id { get; set; }

        public string Name { get; set; }
        
        public string Infos { get; set; }
    }
}