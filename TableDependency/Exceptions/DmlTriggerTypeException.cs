namespace TableDependency.Exceptions
{
    public class DmlTriggerTypeException : TableDependencyException
    {
        protected internal DmlTriggerTypeException(string message = null)
            : base(message)
        { }
    }
}