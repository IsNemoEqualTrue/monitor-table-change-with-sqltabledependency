using System;

namespace ApplicationListener1
{
    public class Product
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public DateTime ExpiringDate { get; set; }
        public decimal Price { get; set; }
    }
}