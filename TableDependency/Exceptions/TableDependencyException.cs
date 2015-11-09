////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;

namespace TableDependency.Exceptions
{
    [Serializable]
    public class TableDependencyException : Exception
    {
        public TableDependencyException()
        { }

        public TableDependencyException(string message, Exception innerException = null)
            : base(message, innerException)
        { }
    }
}