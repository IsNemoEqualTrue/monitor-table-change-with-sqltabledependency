using System.Collections.Generic;

namespace TableDependency.SqlClient.Where.Test.UnitTests.Models
{
    public class Category
    {
        public int Id { get; set; }
        public string Description { get; set; }

        public IList<Product> Products { get; set; } = new List<Product>();
    }
}