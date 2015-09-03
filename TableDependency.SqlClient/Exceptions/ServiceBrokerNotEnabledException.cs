using System;
using TableDependency.Exceptions;

namespace TableDependency.SqlClient.Exceptions
{
    public class ServiceBrokerNotEnabledException : TableDependencyException
    {
        protected internal ServiceBrokerNotEnabledException(Exception innerException = null)
            : base("Service broker not enable.", innerException)
        { }
    }
}