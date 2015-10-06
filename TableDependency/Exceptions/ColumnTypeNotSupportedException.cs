////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;

namespace TableDependency.Exceptions
{
    public class ColumnTypeNotSupportedException : TableDependencyException
    {
        protected internal ColumnTypeNotSupportedException(string message = null, Exception exception = null)
            : base(message, exception)
        { }
    }
}