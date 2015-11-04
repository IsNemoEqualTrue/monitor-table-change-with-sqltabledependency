////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;

namespace TableDependency.Exceptions
{
    [Serializable]
    public class InvalidConnectionStringException : TableDependencyException
    {
        protected internal InvalidConnectionStringException(Exception innerException = null)
            : base("Invalid connection string.", innerException)
        { }
    }
}