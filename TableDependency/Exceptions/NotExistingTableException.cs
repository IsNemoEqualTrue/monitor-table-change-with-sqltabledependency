namespace TableDependency.Exceptions
{
    public class NotExistingTableException : TableDependencyException
    {
        protected internal NotExistingTableException(string tableName)
            : base($"Table '{tableName}' does not exists.")
        { }
    }
}