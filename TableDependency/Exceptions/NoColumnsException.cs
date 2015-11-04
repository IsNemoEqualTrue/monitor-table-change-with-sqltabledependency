////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;

namespace TableDependency.Exceptions
{
    [Serializable]
    public class NoColumnsException : TableDependencyException
    {
        protected internal NoColumnsException(string tableName)
            : base($"No columns for table {tableName}")
        { }
    }
}