namespace TableDependency.Exceptions
{
    public class MessageMisalignedException : TableDependencyException
    {
        protected internal MessageMisalignedException(string message = null)
            : base(message)
        { }
    }
}