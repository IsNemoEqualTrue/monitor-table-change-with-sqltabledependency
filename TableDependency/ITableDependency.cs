////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using TableDependency.Delegates;
using TableDependency.Enums;

namespace TableDependency
{
    public interface ITableDependency<T>
    {
        event ChangedEventHandler<T> OnChanged;
        event ErrorEventHandler OnError;

        void Start(int timeOut = 120, int watchDogTimeOut = 180);
        void Stop();
        TableDependencyStatus Status { get; }
    }
}