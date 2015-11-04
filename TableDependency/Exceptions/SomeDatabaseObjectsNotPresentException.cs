////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;

namespace TableDependency.Exceptions
{
    [Serializable]
    public class SomeDatabaseObjectsNotPresentException : TableDependencyException
    {
        protected internal SomeDatabaseObjectsNotPresentException (string namingConvention)
            : base($"For the given {namingConvention} some expected object is missing")
        { }
    }
}