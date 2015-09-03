namespace TableDependency.Exceptions
{
    public class UpdateOfException : TableDependencyException
    {
        protected internal UpdateOfException(string message)
            : base(message)
        { }
    }
}