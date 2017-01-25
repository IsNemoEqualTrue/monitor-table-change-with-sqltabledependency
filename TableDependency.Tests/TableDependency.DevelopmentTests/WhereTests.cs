using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using TableDependency.SqlClient.Where;

namespace ConsoleApplicationSqlServer
{
    public class Customer
    {
        public int Id { get; set; }
        public string CompanyName { get; set; }
        public string ContactName { get; set; }
    }

    public partial class Program
    {
        public static void TestWhere()
        {
            Expression<Func<Customer, bool>> query = u =>
                u.CompanyName.Trim().Length > 3 &&
                u.CompanyName.Trim() == "WW" &&
                u.CompanyName.Length > 10 &&
                u.CompanyName == "" &&
                u.CompanyName.Contains("WW") &&                
                u.Id > 10;

            var stringSql = new Where().Translate(query);
            Console.WriteLine(stringSql);
            Console.ReadKey();
        }
    }
}
