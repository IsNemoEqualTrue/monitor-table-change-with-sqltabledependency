////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;
using System.Data;

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

            return (SqlDbType)Enum.Parse(typeof(SqlDbType), type, true);
        }         
    }
}