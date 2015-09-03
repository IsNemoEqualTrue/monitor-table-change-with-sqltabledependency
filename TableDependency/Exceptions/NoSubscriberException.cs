using System;

namespace TableDependency.Exceptions
{
    public class NoSubscriberException : TableDependencyException
    {
        protected internal NoSubscriberException(Exception innerException = null)
            : base("No event subscribers registered for receiving notifications. Define an event handler method as event receiver.", innerException)
        { }
    }
}