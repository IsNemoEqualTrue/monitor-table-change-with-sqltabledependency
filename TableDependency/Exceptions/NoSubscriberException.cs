////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;

namespace TableDependency.Exceptions
{
    [Serializable]
    public class NoSubscriberException : TableDependencyException
    {
        protected internal NoSubscriberException(Exception innerException = null)
            : base("No event subscribers registered for receiving notifications. Define an event handler method as event receiver.", innerException)
        { }
    }
}