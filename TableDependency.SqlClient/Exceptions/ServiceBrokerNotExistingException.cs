////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using TableDependency.Exceptions;

namespace TableDependency.SqlClient.Exceptions
{
    public class ServiceBrokerNotExistingException : TableDependencyException
    {
        protected internal ServiceBrokerNotExistingException(string naming)
            : base($"No Service broker with name '{naming}'.")
        { }
    }
}