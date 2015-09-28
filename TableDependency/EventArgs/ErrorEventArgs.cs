////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;

namespace TableDependency.EventArgs
{
    public class ErrorEventArgs : System.EventArgs
    {
        #region Properties

        public string Message { get; private set; }

        public Exception Error { get; protected set; }

        #endregion

        #region Constructors

        internal ErrorEventArgs(Exception e) : this("TableDependency stopped working", e)
        {
        }

        internal ErrorEventArgs(string message, Exception e)
        {
            Message = message;
            Error = e;
        }

        #endregion
    }
}