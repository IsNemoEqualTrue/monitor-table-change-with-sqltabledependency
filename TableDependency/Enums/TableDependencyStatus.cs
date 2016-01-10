////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
namespace TableDependency.Enums
{
    public enum TableDependencyStatus
    {
        None,
        WaitingForStart,
        Starting,
        Started,
        WaitingForNotification,
        StoppedDueToCancellation,
        StoppedDueToError
    }
}