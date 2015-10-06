////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;
using System.Data;
using TableDependency.Exceptions;

namespace TableDependency.SqlClient.Extensions
{
    public static class StringExtension
    {
        public static SqlDbType ToSqlDbType(this string type)
        {
            // Mapping not listed in SqlDbType
            if (type.ToLower() == "numeric")
            {
                return SqlDbType.Decimal;
            }

            try
            {
                return (SqlDbType) Enum.Parse(typeof (SqlDbType), type, true);
            }
            catch (Exception exception)
            {
                throw new ColumnTypeNotSupportedException(exception: exception);
            }
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