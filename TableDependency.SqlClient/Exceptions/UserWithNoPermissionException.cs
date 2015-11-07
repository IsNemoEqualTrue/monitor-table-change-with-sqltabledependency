////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////

using System;
using TableDependency.Exceptions;

namespace TableDependency.SqlClient.Exceptions
{
    [Serializable]
    public class UserWithNoPermissionException : TableDependencyException
    {
        protected internal UserWithNoPermissionException(string missingPermission)
            : base($"User with no {missingPermission} permission.")
        { }

        protected internal UserWithNoPermissionException(Exception innerException = null)
            : base("User with no permission. See inner exception for details.", innerException)
        { }
    }
}