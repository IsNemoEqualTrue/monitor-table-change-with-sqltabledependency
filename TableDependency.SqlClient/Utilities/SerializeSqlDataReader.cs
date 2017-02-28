using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace TableDependency.SqlClient.Utilities
{
    internal class SerializeSqlDataReader
    {
        public static IEnumerable<Dictionary<string, object>> Serialize(SqlDataReader reader)
        {
            var results = new List<Dictionary<string, object>>();
            var cols = new List<string>();
            for (var i = 0; i < reader.FieldCount; i++) cols.Add(reader.GetName(i));

            while (reader.Read()) results.Add(SerializeRow(cols, reader));

            return results;
        }

        private static Dictionary<string, object> SerializeRow(IEnumerable<string> cols, IDataRecord reader)
        {
            return cols.ToDictionary(col => col, col => reader[col]);
        }
    }
}