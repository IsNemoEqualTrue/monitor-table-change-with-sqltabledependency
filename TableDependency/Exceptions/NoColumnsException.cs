////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   © 2015-2106 Christian Del Bianco. All rights reserved.
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