using System;

namespace TableDependency.Exceptions
{
    public class InvalidConnectionStringException : TableDependencyException
    {
        protected internal InvalidConnectionStringException(Exception innerException = null)
            : base("Invalid connection string.", innerException)
        { }
    }
}