////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
namespace TableDependency.Exceptions
{
    public class NoColumnsException : TableDependencyException
    {
        protected internal NoColumnsException(string tableName)
            : base($"No columns for table {tableName}")
        { }
    }
}