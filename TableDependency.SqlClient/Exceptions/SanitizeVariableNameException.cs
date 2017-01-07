using TableDependency.Exceptions;

namespace TableDependency.SqlClient.Exceptions
{
    public class SanitizeVariableNameException : TableDependencyException
    {
        protected internal SanitizeVariableNameException(string tableColumnName)
            : base($"Impossible to define a variable for table column '{tableColumnName}'.")
        { }
    }
}