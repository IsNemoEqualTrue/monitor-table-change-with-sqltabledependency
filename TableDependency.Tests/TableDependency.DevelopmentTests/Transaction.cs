using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApplicationSqlServer
{
    [Table("Item", Schema = "Transaction")]
    public class TransactionItem
    {
        public Guid TransactionItemId { get; set; }
        public string Description { get; set; }
    }
}
