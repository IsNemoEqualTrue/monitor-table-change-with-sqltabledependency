////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   © 2015-2106 Christian Del Bianco. All rights reserved.
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