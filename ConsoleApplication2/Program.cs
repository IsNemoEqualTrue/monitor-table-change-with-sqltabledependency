using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.OracleClient;

namespace ConsoleApplication2
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            OracleTableDependency<Check_Model1> tableDependency = null;
            string naming = null;

            try
            {
                //var mapper = new ModelToTableMapper<Check_Model1>();
                //mapper.AddMapping(c => c.Name, "COLUMN1");
                //mapper.AddMapping(c => c.Id, "COLUMN2");
                //mapper.AddMapping(c => c.Surname, "Long Description");
                //mapper.AddMapping(c => c.Born, "NATO");
                tableDependency = new OracleTableDependency<Check_Model1>(
                    "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=127.0.0.1)(PORT= 1521)))(CONNECT_DATA=(SERVICE_NAME = XE)));User Id=SYSTEM;password=tiger;",
                    "AAA");
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.OnError += TableDependency_OnError;


                tableDependency.Start();

                Console.WriteLine("un tasto per uscire");
                Console.ReadKey();

                tableDependency.Stop();

            }
            finally
            {
                tableDependency?.Dispose();
            }
        }

        private static void TableDependency_OnError(object sender, ErrorEventArgs e)
        {
            Console.WriteLine(e.Error.Message);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Check_Model1> e)
        {
        }
    }
}