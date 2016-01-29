////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   © 2015-2106 Christian Del Bianco. All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;
using TableDependency.Exceptions;

namespace TableDependency.OracleClient.Exceptions
{
    [Serializable]
    public class UserWithNoGrantException : TableDependencyException
    {
        protected internal UserWithNoGrantException(string missingGrant)
            : base($"User with no EXECUTE permission on {missingGrant}.")
        { }
    }
}