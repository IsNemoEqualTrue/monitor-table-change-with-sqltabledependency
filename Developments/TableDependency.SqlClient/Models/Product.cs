using System;

namespace TableDependency.SqlClient.Development.Models
{
    public class Product
    {
        public int Id { get; set; }
        public int CategoryId { get; set; }
        public string Name { get; set; }
        public int? Quantity { get; set; }
        public DateTime Expiring { get; set; }
        public decimal Price { get; set; }
    }
}