using System;
using TableDependency.Exceptions;

namespace TableDependency.SqlClient.Exceptions
{
    public class UserWithNoPermissionException : TableDependencyException
    {
        protected internal UserWithNoPermissionException(Exception innerException = null)
            : base("User with no permission.", innerException)
        { }
    }
}