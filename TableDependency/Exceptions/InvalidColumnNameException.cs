////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
namespace TableDependency.Exceptions
{
    public class InvalidColumnNameException : TableDependencyException
    {
        protected internal InvalidColumnNameException(string tableName, string columName)
            : base($"Does not exists any '{columName}' column name in table {tableName}.")
        { }
    }
}