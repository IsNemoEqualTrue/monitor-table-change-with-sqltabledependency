using System;

namespace ConsoleApplicationSqlServer
{
    public class Customer
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
        public DateTime BirthDay { get; set; }
        public decimal  Salary { get; set; }
    }
}