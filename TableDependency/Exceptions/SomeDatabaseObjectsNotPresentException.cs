////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////

namespace TableDependency.Exceptions
{
    public class SomeDatabaseObjectsNotPresentException : TableDependencyException
    {
        protected internal SomeDatabaseObjectsNotPresentException (string namingConvention)
            : base($"For the given {namingConvention} some expected object is missing")
        { }
    }
}