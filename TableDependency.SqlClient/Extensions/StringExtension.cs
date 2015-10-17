////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;

namespace TableDependency.SqlClient.Extensions
{
    public static class StringExtension
    {
        public static string ConvertNumericType(this string type)
        {
            return type.ToLower() == "numeric" ? "decimal" : type;
        }

        public static bool? ToBoolean(this string str)
        {
            var cleanValue = (str ?? "").Trim();

            if (string.Equals(cleanValue, "0", StringComparison.OrdinalIgnoreCase)) return false;
            if (string.Equals(cleanValue, "1", StringComparison.OrdinalIgnoreCase)) return true;

            return null;
        }
    }
}