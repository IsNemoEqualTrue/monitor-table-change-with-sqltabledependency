////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;
using TableDependency.Exceptions;

namespace TableDependency.SqlClient.Exceptions
{
    [Serializable]
    public class QueueAlreadyUsedException : TableDependencyException
    {
        protected internal QueueAlreadyUsedException(string naming)
            : base($"Already existing objects with naming '{naming}'.")
        { }
    }
}