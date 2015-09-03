using System;

namespace TableDependency.Exceptions
{
    public abstract class TableDependencyException : Exception
    {
        protected TableDependencyException(string message, Exception innerException = null)
            : base(message, innerException)
        { }
    }
}