////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   © 2015-2106 Christian Del Bianco. All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using TableDependency.EventArgs;

namespace TableDependency.Delegates
{
    public delegate void ChangedEventHandler<T>(object sender, RecordChangedEventArgs<T> e) where T : class;
}