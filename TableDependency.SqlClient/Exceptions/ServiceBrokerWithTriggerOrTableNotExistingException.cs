////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using TableDependency.Exceptions;

namespace TableDependency.SqlClient.Exceptions
{
    public class ServiceBrokerWithTriggerOrTableNotExistingException : TableDependencyException
    {
        protected internal ServiceBrokerWithTriggerOrTableNotExistingException(string naming)
            : base($"No trigger or table associated to Service broker with name '{naming}'.")
        { }
    }
}