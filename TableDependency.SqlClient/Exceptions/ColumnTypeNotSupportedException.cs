using TableDependency.Exceptions;

namespace TableDependency.SqlClient.Exceptions
{
    public class ColumnTypeNotSupportedException : TableDependencyException
    {
        protected internal ColumnTypeNotSupportedException(string message)
            : base(message)
        { }
    }
}