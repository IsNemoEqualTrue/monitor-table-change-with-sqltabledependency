using System;
using System.Collections.Generic;

namespace TableDependency.SqlClient.Base.Health;

public enum DatabaseObjectKind
{
    Broker,
    MessageType,
    Contract,
    Queue,
    Service,
    Procedure,
    QueueActivation,
    Trigger,
    Conversation
}

public enum DatabaseObjectProblem
{
    Missing,
    Disabled
}

public sealed record DatabaseObjectIssue(DatabaseObjectKind Kind, string Name, DatabaseObjectProblem Problem)
{
    public override string ToString() => $"{Kind} {Name} {Problem}";
}

/// <summary>Result of <see cref="Interfaces.ITableDependency{T}.CheckDatabaseObjectsAsync"/>.</summary>
/// <param name="CheckedAt">When the check ran.</param>
/// <param name="Listening">The receive loop is in WAITFOR (not stopped or reconnecting).</param>
/// <param name="Issues">What was wrong when checked, before any repair.</param>
/// <param name="Remaining">What is still wrong after repair; equals <paramref name="Issues"/> when nothing was repaired.</param>
/// <param name="RepairError">Why repair failed, if it did.</param>
public sealed record DatabaseObjectsHealth(
    DateTimeOffset CheckedAt,
    bool Listening,
    IReadOnlyList<DatabaseObjectIssue> Issues,
    IReadOnlyList<DatabaseObjectIssue> Remaining,
    Exception? RepairError)
{
    public bool IsHealthy => Listening && Remaining.Count is 0 && RepairError is null;
}