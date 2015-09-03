using TableDependency.Exceptions;

namespace TableDependency.SqlClient.Exceptions
{
    public class ServiceBrokerErrorMessageException : TableDependencyException
    {
        protected internal ServiceBrokerErrorMessageException(string naming)
            : base($"Service broker {naming} send an error message.")
        { }
    }
}
