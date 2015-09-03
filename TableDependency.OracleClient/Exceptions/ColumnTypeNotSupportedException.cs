using TableDependency.Exceptions;

namespace TableDependency.OracleClient.Exceptions
{
    public class ColumnTypeNotSupportedException : TableDependencyException
    {
        protected internal ColumnTypeNotSupportedException(string table, string type)
            : base($"{table} table has a column of {type} type. This type is not supported by OracleTableDependency.")
        { }
    }
}