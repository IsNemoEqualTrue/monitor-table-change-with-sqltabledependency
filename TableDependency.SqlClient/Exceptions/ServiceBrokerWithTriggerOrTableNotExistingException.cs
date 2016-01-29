////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   © 2015-2106 Christian Del Bianco. All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;
using TableDependency.Exceptions;

namespace TableDependency.SqlClient.Exceptions
{
    [Serializable]
    public class ServiceBrokerWithTriggerOrTableNotExistingException : TableDependencyException
    {
        protected internal ServiceBrokerWithTriggerOrTableNotExistingException(string naming)
            : base($"No trigger or table associated to Service broker with name '{naming}'.")
        { }
    }
}