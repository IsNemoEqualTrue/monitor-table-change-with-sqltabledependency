////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   © 2015-2106 Christian Del Bianco. All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;
using TableDependency.Exceptions;

namespace TableDependency.SqlClient.Exceptions
{
    [Serializable]
    public class ServiceBrokerNotExistingException : TableDependencyException
    {
        protected internal ServiceBrokerNotExistingException(string naming)
            : base($"No Service broker with name '{naming}'.")
        { }
    }
}