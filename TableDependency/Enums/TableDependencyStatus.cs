////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
namespace TableDependency.Enums
{
    public enum TableDependencyStatus
    {
        None = 0,
        WaitingToStart = 1,
        Starting = 2,
        Started = 3,
        ListenerForNotification = 4,
        StoppedDueToCancellation = 5,
        StoppedDueToError = 6
    }
}