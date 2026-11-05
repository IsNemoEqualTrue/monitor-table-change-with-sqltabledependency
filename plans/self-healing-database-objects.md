# Plan: monitor and self-heal SqlTableDependency database objects

Release: **10.2.0** (minor; current `<Version>` is 10.1.9). No existing member changes signature; new interface members get default implementations so third-party `ITableDependency<T>` implementations still compile.

## Problem

A listener can believe it is healthy while the objects it depends on are gone. Expected causes:

- The database is reverted from a snapshot (or restored) while the app keeps running.
- Someone drops or disables the trigger, a queue, a service, the contract or the activation procedure by hand.
- Service Broker is disabled on the database (a restore/revert leaves `is_broker_enabled = 0`).

All of these must recover without restarting the host process.

## What happens today (evidence)

| Event | Library behaviour today | Visible? |
| --- | --- | --- |
| Snapshot revert / restore | Revert needs exclusive access, so the `WAITFOR` session is killed (`SqlException`: "session is in the kill state"). Loop exits with `OnException` + `StopDueToError`. Nothing restarts inside the library. | Yes, once - then silent |
| Restart after revert, broker off | `ConfigureAsync` throws `ServiceBrokerNotEnabledException`. | Yes |
| Trigger dropped/disabled, queues intact | `WAITFOR (RECEIVE)` keeps returning empty at its timeout. Status stays `WaitingForNotification`. | **No - silent forever** |
| Receiver queue dropped | `RECEIVE` fails, loop exits with `StopDueToError`. | Yes, once |
| Service/contract/message type dropped, trigger intact | Trigger's `SEND` fails and its `CATCH` re-raises (`SqlScripts.CreateTrigger`), so **the application's own INSERT/UPDATE/DELETE on the table fails**. | Only to the writer |
| Activation proc dropped | Timer/end-dialog messages pile up on the `_Sender` queue; dialogs never retire. | No |

Rebuilding is already idempotent: message types, contract, queues and services are `IF NOT EXISTS`; trigger and procedure are `CREATE OR ALTER`; queue activation is `ALTER QUEUE`. Persisted mode reuses or re-begins the conversation by name (`ReconnectToPersistedDatabaseObjectsAsync`). `CheckIfDatabaseObjectsExistAsync` only checks queues today.

## Design

### 1. Health check, driven by the caller's heartbeat

The library does **not** poll on its own. The host calls the check from its heartbeat; each call checks and, when `AutoRepair` is on, repairs.

```csharp
// ITableDependency<T> (default interface implementations for compatibility)
bool AutoRepair { get; set; }                                   // default true
Task<DatabaseObjectsHealth> CheckDatabaseObjectsAsync(CancellationToken ct = default);
DatabaseObjectsHealth? LastObjectsHealth { get; }              // most recent result, null before first call
```

```csharp
public sealed record DatabaseObjectsHealth(
    DateTimeOffset CheckedAt,
    bool BrokerEnabled,
    bool Listening,                                  // receive loop is in WAITFOR (not stopped/reconnecting)
    IReadOnlyList<DatabaseObjectIssue> Issues,       // what was wrong when checked
    IReadOnlyList<DatabaseObjectIssue> Repaired,     // what this call fixed
    Exception? RepairError)                          // why repair failed, if it did
{
    public bool IsHealthy => BrokerEnabled && Listening && Issues.Count == Repaired.Count && RepairError is null;
}

public sealed record DatabaseObjectIssue(DatabaseObjectKind Kind, string Name, DatabaseObjectProblem Problem);
// Kind: Broker, MessageType, Contract, Queue, Service, Procedure, QueueActivation, Trigger, Conversation
// Problem: Missing, Disabled
```

Heartbeat example for the README:

```csharp
var health = await dependency.CheckDatabaseObjectsAsync(ct);
if (!health.IsHealthy)
    logger.LogError("Listener unhealthy: {Issues}", health.Issues);
```

Check query: one read-only batch in `SqlScripts`, names bound as parameters, `COLLATE DATABASE_DEFAULT` on broker catalog names. It covers `sys.databases.is_broker_enabled`, trigger exists + `is_disabled = 0`, both queues exist + `is_receive_enabled`/`is_enqueue_enabled` + `is_activation_enabled` on the sender queue, both services, the contract, every message type, the activation procedure, and an initiator conversation not in `CLOSED`/`ERROR` (persisted: found by service name; non-persisted: `_conversationHandle`). Runs on its own short-lived connection, so it works while the receive loop is reconnecting.

Telemetry on the existing `ActivitySource`: span `CheckDatabaseObjects` with tags `tabledependency.objects.healthy`, `tabledependency.objects.issues`, `tabledependency.objects.repaired`. Warning log listing issues; Information log listing repairs.

Concurrency: a `SemaphoreSlim` serialises check/repair calls and repair against the receive loop's own reconnect (section 3), so two heartbeats or a heartbeat and a reconnect never rebuild at the same time. A call made while another holds the lock waits up to its `ct`.

### 2. Repair (`AutoRepair`, default `true`)

Expected deployment: the environment's reset/provisioning script re-enables the broker and re-applies the grants after a snapshot revert. The library then only has to rebuild its own objects, which needs no permissions beyond the existing guard. Broker re-enable from the library is best effort: it needs `ALTER` on the database, which a least-privilege principal does not hold, so it normally lands in `RepairError` until the script has run and the next heartbeat repairs the rest. Permissions or the database user lost to a revert are out of scope - the library cannot repair what it no longer has rights to.

When the check finds issues and `AutoRepair` is true:

1. **Broker disabled** - try `ALTER DATABASE CURRENT SET ENABLE_BROKER WITH NO_WAIT`. `NO_WAIT` fails immediately rather than killing other sessions if exclusive access is not available. If it fails with 9772 (broker GUID already in use, typical after a restore), retry with `NEW_BROKER WITH NO_WAIT` (ends all existing conversations in the database - acceptable, they are already dead after a revert). If both fail (most often: no `ALTER` on the database, or other sessions open), report `RepairError` and stop this repair; the next heartbeat tries again. Never `ROLLBACK IMMEDIATE`.
2. **Any object missing/disabled** - re-run the existing `CreateDatabaseObjectsAsync` body (idempotent) in one transaction; `ENABLE TRIGGER` if it exists but is disabled; `ALTER QUEUE ... STATUS = ON` if disabled.
3. **Persisted** - reconnect the conversation by name (existing `ReconnectToPersistedDatabaseObjectsAsync`).
4. **Non-persisted** - the trigger embeds the conversation handle, so: `BEGIN DIALOG` a new conversation, re-create the trigger with the new handle, re-arm the watchdog timer, and swap `_conversationHandle` under the lock so the receive loop arms the timer on the new dialog. `NamingPrefix` is unchanged for the life of the instance.
5. Re-run the check and return it, with `Repaired` filled in. Raise `OnStatusChanged(Repaired)` (new enum value, appended so existing numeric values do not move).

Repair failures never stop the receive loop; they surface in `RepairError` and through `OnException`.

`AutoRepair = false`: check and report only.

### 3. Reconnect after a killed connection (snapshot revert)

Today a severed connection ends `WaitForNotificationsAsync`. New behaviour: on a `SqlException` where the listener's connection is `Closed`/`Broken`, with no cancellation and no shutdown latch:

1. Raise `OnException` (Warning-level log, not Error) and `OnStatusChanged(Reconnecting)` (new enum value).
2. Back off 1s doubling up to 60s; on each attempt take the repair lock, run check + repair (honouring `AutoRepair`; with it off, only reconnect when objects are intact), then resume `WAITFOR` and raise `WaitingForNotification`.
3. If not back within `ReconnectTimeout` (new property, default 30 min, `Timeout.InfiniteTimeSpan` allowed), stop with `StopDueToError` exactly as today.

Subscriber/mapper faults keep their current behaviour (`State` is not `Closed`/`Broken`, so they still surface as `StopDueToError`) - same discriminator the shutdown fast path already uses. The `_shuttingDown` latch still wins, so a SIGTERM during an outage reports `StopDueToProcessShutdown`.

Non-persisted: on teardown the loop's `finally` currently drops objects; it must only do that on a real stop, not between reconnect attempts.

## Tests (xUnit, `// ARRANGE / ACT / ASSERT`, persisted and non-persisted where it applies)

- `CheckDatabaseObjects_ReportsHealthy_WhenListening`
- `CheckDatabaseObjects_ReportsIssue_ForEachObjectKind` (theory: drop/disable each kind)
- `CheckDatabaseObjects_AutoRepairOff_ReportsWithoutRepairing`
- `CheckDatabaseObjects_RebuildsDroppedTrigger_AndNextInsertIsDelivered`
- `CheckDatabaseObjects_RebuildsDroppedService_WriterNoLongerFails`
- `CheckDatabaseObjects_EnablesBroker_WhenPermitted`
- `CheckDatabaseObjects_ReportsRepairError_WhenBrokerCannotBeEnabled` (principal without db `ALTER`)
- `CheckDatabaseObjects_ConcurrentCalls_RepairOnce`
- `Listener_ReconnectsAfterSessionKill` (`KILL` the listener's spid, then insert)
- `Listener_SurvivesDatabaseSnapshotRevert` (snapshot before start, revert, heartbeat-check, insert, assert delivered)
- `Listener_StopsWithError_AfterReconnectTimeout`
- `Listener_SubscriberFault_StillStopsWithError` (regression)
- `Listener_SigtermDuringReconnect_ReportsProcessShutdown` (regression)
- Backoff ladder unit test using an injected delay, as `SpinGuardTest` does.

## Docs

- README: "Monitoring and self-healing" section with the heartbeat example, `AutoRepair`, `ReconnectTimeout`, and the permission needed for broker re-enable (`ALTER` on the database; without it the broker must be re-enabled by a DBA and the next heartbeat repairs the rest).
- AGENTS.md: repair lock, `NO_WAIT` (never `ROLLBACK IMMEDIATE`), non-persisted handle swap, and that reconnect must not run the drop-all `finally`.
- Bump `<Version>` to `10.2.0`.

## Risks

- `NEW_BROKER` ends every conversation in the database, including ones owned by other apps. Only attempted after `ENABLE_BROKER` fails with 9772, which means the broker is unusable anyway.
- Hosts that restart on `StopDueToError` will now see it only after `ReconnectTimeout`; release notes must call this out.
- A DBA who drops the trigger on purpose will see it come back on the next heartbeat while `AutoRepair` is on.

## Review outcome (implemented)

Changes from the design above, and why:

- **Broker re-enable only when permitted.** It is attempted only if `HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'ALTER') = 1`; otherwise `ServiceBrokerNotEnabledException` goes in `RepairError`. It is issued from `master` after clearing the library's own pool, so neither the issuing session nor idle pooled connections block the `NO_WAIT` exclusive lock. Objects are rebuilt even if the broker stays off.
- **`DatabaseObjectsHealth` reports `Remaining` (re-checked after repair) instead of `Repaired` + `BrokerEnabled`.** `IsHealthy => Listening && Remaining.Count == 0 && RepairError is null`. The old `Issues.Count == Repaired.Count` formula broke when a repair fixed one thing and broke another.
- **No `Repaired` status.** Status is a state: after a repair it would stay `Repaired` while the loop is waiting. Only `Reconnecting` is added.
- **Reconnect does not raise `OnException`.** That event means "stopped working". It fires once, with `StopDueToError`, when `ReconnectTimeout` runs out. `TimeSpan.Zero` keeps the pre-10.2 immediate stop.
- **Retry rule: a `SqlException` raised outside subscriber dispatch.** This replaces "connection Closed/Broken": a dropped receiver queue fails with the connection still `Open` and would have left a dead loop that no heartbeat could revive.
- **Repair only while the loop runs, under the same lock as the non-persisted final drop.** Otherwise a heartbeat racing `StopAsync` recreates the objects that were just dropped.
- **Non-persisted dialog.** Reuse it while open. When replacing it, end the old one `WITH CLEANUP` (otherwise its timer later fires the activation drop-all). Publish the new handle only after commit, via a fresh box, so the loop never reads a torn or rolled-back handle.
- **`ENABLE TRIGGER` and `ALTER QUEUE ... STATUS = ON` always run.** `CREATE OR ALTER TRIGGER` keeps a disabled trigger disabled.
- **`AutoRepair`/`ReconnectTimeout` live on the class and the builder, not the interface.** Interface default setters would silently drop values. `CheckDatabaseObjectsAsync`/`LastObjectsHealth` are default interface members.
- **Tests.** The snapshot-revert test was replaced: reverting the shared fixture database would break parallel tests. `Listener_RebuildsObjectsDroppedDuringOutage` disables the login, kills the session and drops the objects, which is the same end state. Every DB test runs under its own login.