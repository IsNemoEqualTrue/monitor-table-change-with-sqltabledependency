////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using TableDependency.EventArgs;

namespace TableDependency.Delegates
{
    public delegate void ChangedEventHandler<T>(object sender, RecordChangedEventArgs<T> e);
}