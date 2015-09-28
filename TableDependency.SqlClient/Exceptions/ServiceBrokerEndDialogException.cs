////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using TableDependency.Exceptions;

namespace TableDependency.SqlClient.Exceptions
{
    public class ServiceBrokerEndDialogException : TableDependencyException
    {
        protected internal ServiceBrokerEndDialogException(string naming)
            : base($"Service broker {naming} ended the conversation.")
        { }
    }
}