using Microsoft.Data.SqlClient;
using System;
using System.Threading;

namespace TableDependency.SqlClient;

/// <summary>
/// Decides whether the receive loop retries after a failure, and how long it waits between attempts.
/// Static so <c>ReconnectPolicyTest</c> can assert the rules without a database.
/// </summary>
internal static class ReconnectPolicy
{
    internal static readonly TimeSpan BackoffMax = TimeSpan.FromSeconds(60);

    /// <summary>Doubles from one second up to <see cref="BackoffMax"/>.</summary>
    internal static TimeSpan NextDelay(TimeSpan previous)
        => SpinGuard.NextSpinBackoff(previous, BackoffMax);

    /// <summary>
    /// Only a failure of the receive itself is retried: a severed or killed connection, or a broker object that went missing under it.
    /// A subscriber fault (even a <see cref="SqlException"/> thrown from a handler) or a non-SQL failure still stops the listener.
    /// <see cref="TimeSpan.Zero"/> disables reconnecting; <see cref="Timeout.InfiniteTimeSpan"/> retries forever.
    /// </summary>
    internal static bool ShouldReconnect(Exception exception, bool dispatching, TimeSpan outage, TimeSpan reconnectTimeout)
        => exception is SqlException
           && !dispatching
           && reconnectTimeout != TimeSpan.Zero
           && (reconnectTimeout == Timeout.InfiniteTimeSpan || outage < reconnectTimeout);
}