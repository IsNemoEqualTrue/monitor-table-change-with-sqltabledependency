using TableDependency.SqlClient.Base.Exceptions;
using TableDependency.SqlClient.Messages;

namespace TableDependency.SqlClient.Exceptions
{
    public class QueueContainingErrorMessageException : TableDependencyException
    {
        protected internal QueueContainingErrorMessageException()
            : base($"Queue containig a '{SqlMessageTypes.ErrorType}' message.")
        { }
    }
}