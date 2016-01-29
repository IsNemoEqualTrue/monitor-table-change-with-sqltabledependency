////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   © 2015-2106 Christian Del Bianco. All rights reserved.
////////////////////////////////////////////////////////////////////////////////

using System;

namespace TableDependency.Exceptions
{
    [Serializable]
    public class InvalidColumnNameException : TableDependencyException
    {
        protected internal InvalidColumnNameException(string tableName, string columName)
            : base($"Does not exists any '{columName}' column name in table {tableName}.")
        { }
    }
}