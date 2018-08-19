using System.Collections.Generic;

namespace TableDependency.SqlClient.Test.Client.Framework.Models
{
    public class Category
    {
        public int Id { get; set; }
        public string Name { get; set; }

        public IList<Product> Products { get; set; } = new List<Product>();
    }
}