////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;

namespace TableDependency.Exceptions
{
    [Serializable]
    public class DmlTriggerTypeException : TableDependencyException
    {
        protected internal DmlTriggerTypeException(string message = null)
            : base(message)
        { }
    }
}