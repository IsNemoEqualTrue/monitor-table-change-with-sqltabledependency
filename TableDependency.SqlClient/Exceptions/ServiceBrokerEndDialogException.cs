////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;
using TableDependency.Exceptions;

namespace TableDependency.SqlClient.Exceptions
{
    [Serializable]
    public class ServiceBrokerEndDialogException : TableDependencyException
    {
        protected internal ServiceBrokerEndDialogException(string naming)
            : base($"Service broker {naming} ended the conversation.")
        { }
    }
}