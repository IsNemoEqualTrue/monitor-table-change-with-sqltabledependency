////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;

namespace TableDependency.Exceptions
{
    [Serializable]
    public class MessageMisalignedException : TableDependencyException
    {
        protected internal MessageMisalignedException(string message = null)
            : base(message)
        { }
    }
}