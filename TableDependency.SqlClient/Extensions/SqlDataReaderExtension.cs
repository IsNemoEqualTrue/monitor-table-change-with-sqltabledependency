////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;
using System.Data.SqlClient;

namespace TableDependency.SqlClient.Extensions
{
    public static class SqlDataReaderExtension
    {
        public static string GetSafeString(this SqlDataReader reader, int columnIndex)
        {
            return !reader.IsDBNull(columnIndex) ? Convert.ChangeType(reader[columnIndex], typeof(string)) as string : null;
        }
    }
}