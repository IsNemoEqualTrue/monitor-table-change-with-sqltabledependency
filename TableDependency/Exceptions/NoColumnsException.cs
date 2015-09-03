namespace TableDependency.Exceptions
{
    public class NoColumnsException : TableDependencyException
    {
        protected internal NoColumnsException(string tableName)
            : base($"No columns for table {tableName}")
        { }
    }
}