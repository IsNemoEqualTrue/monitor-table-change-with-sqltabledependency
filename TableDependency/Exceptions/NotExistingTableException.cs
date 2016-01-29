////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   © 2015-2106 Christian Del Bianco. All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;

namespace TableDependency.Exceptions
{
    [Serializable]
    public class NotExistingTableException : TableDependencyException
    {
        protected internal NotExistingTableException(string tableName)
            : base($"Table '{tableName}' does not exists.")
        { }
    }
}