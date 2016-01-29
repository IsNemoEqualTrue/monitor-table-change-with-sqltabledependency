////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   © 2015-2106 Christian Del Bianco. All rights reserved.
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