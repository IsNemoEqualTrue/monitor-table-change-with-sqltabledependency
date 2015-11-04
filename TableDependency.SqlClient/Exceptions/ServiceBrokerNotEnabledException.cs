////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;
using TableDependency.Exceptions;

namespace TableDependency.SqlClient.Exceptions
{
    [Serializable]
    public class ServiceBrokerNotEnabledException : TableDependencyException
    {
        protected internal ServiceBrokerNotEnabledException(Exception innerException = null)
            : base("Service broker not enable.", innerException)
        { }
    }
}