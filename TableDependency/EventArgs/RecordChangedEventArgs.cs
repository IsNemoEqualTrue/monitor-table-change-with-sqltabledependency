using TableDependency.Enums;

namespace TableDependency.EventArgs
{
    ////////////////////////////////////////////////////////////////////////////////
    //   TableDependency, SqlTableDependency, OracleTableDependency
    //   Copyright (c) Christian Del Bianco.  All rights reserved.
    ////////////////////////////////////////////////////////////////////////////////
    public abstract class RecordChangedEventArgs<T> : System.EventArgs
    {
        public abstract T Entity { get; protected set; }
        public abstract ChangeType ChangeType { get; protected set; }
        public abstract string MessageType { get; protected set; }
    }
}