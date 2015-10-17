using System.ComponentModel.DataAnnotations.Schema;

namespace TableDependency.IntegrationTest.Models
{
    public class Item3
    {
        public long Id { get; set; }

        public string Name { get; set; }

        [Column(ColumnName)]
        public string FamilyName { get; set; }


        private const string ColumnName = "SURNAME";
        public static string GetColumnName => ColumnName;
    }
}