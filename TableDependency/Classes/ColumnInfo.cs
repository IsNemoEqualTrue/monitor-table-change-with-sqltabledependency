////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   © 2015-2106 Christian Del Bianco. All rights reserved.
////////////////////////////////////////////////////////////////////////////////
namespace TableDependency.Classes
{
    public class ColumnInfo
    {
        public ColumnInfo(string name, string type, string size = null)
        {
            this.Name = name;
            this.Type = type;
            this.Size = size;
        }

        public string Name { get; set; }
        public string Type { get; set; }
        public string Size { get; set; }
    }
}