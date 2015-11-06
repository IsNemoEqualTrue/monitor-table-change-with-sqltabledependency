////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;
using TableDependency.Exceptions;

namespace TableDependency.OracleClient.Exceptions
{
    [Serializable]
    public class UserWithNoPermissionException : TableDependencyException
    {
        protected internal UserWithNoPermissionException(string missingPermission)
            : base($"User with no {missingPermission} permission.")
        { }
    }
}