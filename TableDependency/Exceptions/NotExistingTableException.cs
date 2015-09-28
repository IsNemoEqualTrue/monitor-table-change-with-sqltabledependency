////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
namespace TableDependency.Exceptions
{
    public class NotExistingTableException : TableDependencyException
    {
        protected internal NotExistingTableException(string tableName)
            : base($"Table '{tableName}' does not exists.")
        { }
    }
}