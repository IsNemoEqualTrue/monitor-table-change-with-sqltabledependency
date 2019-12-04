using TableDependency.SqlClient.Base.Exceptions;
using TableDependency.SqlClient.Messages;

namespace TableDependency.SqlClient.Exceptions
{
    public class QueueContainingErrorMessageException : TableDependencyException
    {
        public QueueContainingErrorMessageException() : base($"Queue containig a '{SqlMessageTypes.ErrorType}' message.")
        { 
        }
    }
}