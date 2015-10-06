////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////

using System.Data.SqlClient;

namespace TableDependency.SqlClient.Extensions
{
    public static class SqlDataReaderExtension
    {
        public static string GetSafeString(this SqlDataReader reader, int columnIndex)
        {
            if (reader.IsDBNull(columnIndex)) return null;            
            var characterMaximumLength = reader.GetValue(columnIndex);
            return characterMaximumLength?.ToString();
        }
    }
}