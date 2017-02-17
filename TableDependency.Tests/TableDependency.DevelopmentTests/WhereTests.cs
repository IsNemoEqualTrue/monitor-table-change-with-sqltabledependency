using System;
using System.Linq.Expressions;

namespace ConsoleApplicationSqlServer
{
    public partial class Program
    {
        public static void TestWhere()
        {
            //Expression<Func<Customers, bool>> query = u =>
            //    //u.CompanyName.EndsWith("a")
            //    u.CompanyName.Substring(1, 4).Trim().Length > 3
            //    //&& u.CompanyName.Trim() == "WW"
            //    //&& u.CompanyName.Length > 10
            //    //&& u.CompanyName == ""
            //    //&& u.CompanyName == string.Empty
            //    //&& u.CompanyName.Contains("WW")
            //    //&& u.Id > 10;
            //    ;

            //var stringSql = new Where().Translate(query);
            //Console.WriteLine(stringSql);
            //Console.ReadKey();
        }
    }
}
