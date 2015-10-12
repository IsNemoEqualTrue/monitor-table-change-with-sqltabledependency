using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.OracleClient;

namespace ConsoleApplication1
{
    public class Item
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
    }

    internal class Program
    {
        private static string _connectionString = "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=127.0.0.1)(PORT= 1521)))(CONNECT_DATA=(SERVICE_NAME = XE)));User Id=SYSTEM;password=tiger;";
        private static string TableName = "AAA";

        private static void Main(string[] args)
        {
            OracleTableDependency<Item> tableDependency = null;
            string naming = null;

            try
            {
                var mapper = new ModelToTableMapper<Item>();
                mapper.AddMapping(c => c.Description, "Long Description");

                tableDependency = new OracleTableDependency<Item>(_connectionString, TableName, mapper);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.OnError += TableDependency_OnError;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Console.WriteLine("Un tasto per uscire");
                Console.ReadKey();
            }
            finally
            {
                tableDependency?.Dispose();
            }
        }

        private static void TableDependency_OnError(object sender, ErrorEventArgs e)
        {
            throw e.Error;
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Item> e)
        {
            Console.WriteLine(e.Entity.Id);
            Console.WriteLine(e.Entity.Name);
            Console.WriteLine(e.Entity.Description);
            Console.WriteLine("");
        }
    }
}
