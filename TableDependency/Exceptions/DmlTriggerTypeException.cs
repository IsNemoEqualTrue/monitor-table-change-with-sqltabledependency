////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   © 2015-2106 Christian Del Bianco. All rights reserved.
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