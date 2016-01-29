////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   © 2015-2106 Christian Del Bianco. All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;

namespace TableDependency.Exceptions
{
    [Serializable]
    public class ModelToTableMapperException : TableDependencyException
    {
        protected internal ModelToTableMapperException()
            : base("Invalid mapping.")
        { }

        protected internal ModelToTableMapperException(string message)
            : base(message)
        { }
    }
}