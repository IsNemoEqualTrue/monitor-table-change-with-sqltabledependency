////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   © 2015-2106 Christian Del Bianco. All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;

namespace TableDependency.Exceptions
{
    [Serializable]
    public class ColumnTypeNotSupportedException : TableDependencyException
    {
        protected internal ColumnTypeNotSupportedException(string message = null, Exception exception = null)
            : base(message, exception)
        { }
    }
}