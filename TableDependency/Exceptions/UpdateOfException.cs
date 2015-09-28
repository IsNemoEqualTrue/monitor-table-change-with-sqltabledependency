////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
namespace TableDependency.Exceptions
{
    public class UpdateOfException : TableDependencyException
    {
        protected internal UpdateOfException(string message)
            : base(message)
        { }
    }
}