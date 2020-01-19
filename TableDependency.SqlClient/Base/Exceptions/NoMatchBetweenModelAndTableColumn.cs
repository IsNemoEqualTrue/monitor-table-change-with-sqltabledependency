namespace TableDependency.SqlClient.Base.Exceptions
{
    public class NoMatchBetweenModelAndTableColumn : TableDependencyException
    {
        public NoMatchBetweenModelAndTableColumn(string modelProperty)
            : base("Property {modelProperty} in your C# model has a value that is not compatible with the matching table columns.")
        { }
    }
}