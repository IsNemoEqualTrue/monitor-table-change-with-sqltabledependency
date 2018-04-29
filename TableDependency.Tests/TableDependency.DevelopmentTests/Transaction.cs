using System;
using System.ComponentModel.DataAnnotations.Schema;

namespace ConsoleApplicationSqlServer
{
    [Table("Item", Schema = "Transaction")]
    public class TransactionItem
    {
        public Guid TransactionItemId { get; set; }
        public string Description { get; set; }
    }
}
