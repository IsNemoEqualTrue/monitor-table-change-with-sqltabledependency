////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System.ComponentModel;

namespace TableDependency.SqlClient.TypeConverters
{
    [TypeConverter(typeof (SqlBooleanConverter))]
    internal struct SqlBooleanConverterAdapter
    {
    }
}