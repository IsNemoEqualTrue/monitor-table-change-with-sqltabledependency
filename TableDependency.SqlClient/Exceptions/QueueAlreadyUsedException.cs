////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using TableDependency.Exceptions;

namespace TableDependency.SqlClient.Exceptions
{
    public class QueueAlreadyUsedException : TableDependencyException
    {
        protected internal QueueAlreadyUsedException(string naming)
            : base($"Already existing objects with naming '{naming}'.")
        { }
    }
}