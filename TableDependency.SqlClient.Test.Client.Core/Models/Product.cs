using System;

namespace TableDependency.SqlClient.Test.Client.Core.Models
{
    public class Product
    {
        public int Quantity { get; set; }
        public int Id { get; set; }
        public int CategoryId { get; set; }
        public string Name { get; set; }
        public DateTime Expiring { get; set; }
        public decimal Price { get; set; }
    }
}