#region License

// TableDependency, SqlTableDependency
// Copyright (c) 2015-2020 Christian Del Bianco. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation
// files (the "Software"), to deal in the Software without
// restriction, including without limitation the rights to use,
// copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.

#endregion

using Microsoft.Data.SqlClient;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test.Features.Lifecycle;

// On container stop the token cancels the WAITFOR; the resulting exception must not escape StartAsync into the worker's catch.
public sealed class PersistedWaitForStopCancellationDoesNotEscapeTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private const string TableName = nameof(PersistedWaitForStopCancellationDoesNotEscapeTest);

    private sealed class Model
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}] ([Id] INT IDENTITY(1, 1) NOT NULL PRIMARY KEY, [Name] NVARCHAR(100) NOT NULL);";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    public override async ValueTask DisposeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
    }

    [Fact]
    public async Task PersistedListener_CancelledDuringWaitForStop_CompletesWithoutEscapingException()
    {
        // ARRANGE
        var persistentId = $"shutdown_{Guid.NewGuid():N}";
        using var shutdownCts = new CancellationTokenSource();
        var waiting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Exception? raisedException = null;

        var dependency = await SqlTableDependency<Model>.CreateSqlTableDependencyAsync(
            DependencyConnectionString,
            tableName: TableName,
            persistentId: persistentId,
            ct: TestContext.Current.CancellationToken);
        dependency.ReconnectTimeout = TimeSpan.Zero; // asserts the immediate stop, not the reconnect

        try
        {
            dependency.OnChanged += _ => { };
            dependency.OnStatusChanged += e =>
            {
                if (e.Status is TableDependencyStatus.WaitingForNotification)
                    waiting.TrySetResult();
            };
            dependency.OnExceptionAsync = (ExceptionEventArgs e) => { raisedException = e.Exception; return Task.CompletedTask; };

            // ACT
            var listening = dependency.StartAsync(timeout: 60, watchdogTimeout: 120, waitForStop: true, ct: shutdownCts.Token);
            await waiting.Task.WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
            await shutdownCts.CancelAsync();

            // ASSERT
            // The cancelled WAITFOR is benign: the task completes, nothing propagates to the caller's catch
            await listening.WaitAsync(TimeSpan.FromSeconds(90), TestContext.Current.CancellationToken);
            Assert.Equal(TableDependencyStatus.StopDueToCancellation, dependency.Status);
            Assert.Null(raisedException);
        }
        finally
        {
            await dependency.DisposeAsync();
            await dependency.DropDatabaseObjectsAsync();
        }
    }

    [Fact]
    public async Task PersistedListener_ConnectionSeveredWithoutCancellation_SurfacesViaOnExceptionWithoutEscaping()
    {
        // ARRANGE
        var persistentId = $"severed_{Guid.NewGuid():N}";
        var waiting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Exception? raisedException = null;

        var dependency = await SqlTableDependency<Model>.CreateSqlTableDependencyAsync(
            DependencyConnectionString,
            tableName: TableName,
            persistentId: persistentId,
            ct: TestContext.Current.CancellationToken);
        dependency.ReconnectTimeout = TimeSpan.Zero; // asserts the immediate stop, not the reconnect

        bool escaped;
        try
        {
            dependency.OnChanged += _ => { };
            dependency.OnStatusChanged += e =>
            {
                if (e.Status is TableDependencyStatus.WaitingForNotification)
                    waiting.TrySetResult();
            };
            dependency.OnExceptionAsync = (ExceptionEventArgs e) => { raisedException = e.Exception; return Task.CompletedTask; };

            // ACT
            // WaitForStop with a never-cancelled token, then sever the connection (network drop, not a graceful stop)
            var listening = dependency.StartAsync(timeout: 60, watchdogTimeout: 120, waitForStop: true, ct: TestContext.Current.CancellationToken);
            await waiting.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
            await KillSqlTableDependencyDbConnection(TestContext.Current.CancellationToken);

            // ASSERT
            // Does the SqlException escape StartAsync, or is it caught and surfaced via OnException?
            try
            {
                await listening.WaitAsync(TimeSpan.FromSeconds(90), TestContext.Current.CancellationToken);
                escaped = false;
            }
            catch (SqlException)
            {
                escaped = true;
            }
        }
        finally
        {
            await dependency.DisposeAsync();
            await dependency.DropDatabaseObjectsAsync();
        }

        Assert.False(escaped);
        Assert.IsType<SqlException>(raisedException);
        Assert.Equal(TableDependencyStatus.StopDueToError, dependency.Status);
    }

    [Fact]
    public async Task PersistedListener_ConnectionSeveredWhileShuttingDown_IsTreatedAsCleanStop()
    {
        // ARRANGE
        var persistentId = $"sigterm_{Guid.NewGuid():N}";
        var waiting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Exception? raisedException = null;

        var dependency = await SqlTableDependency<Model>.CreateSqlTableDependencyAsync(
            DependencyConnectionString,
            tableName: TableName,
            persistentId: persistentId,
            ct: TestContext.Current.CancellationToken);
        dependency.ReconnectTimeout = TimeSpan.Zero; // asserts the immediate stop, not the reconnect

        try
        {
            dependency.OnChanged += _ => { };
            dependency.OnStatusChanged += e =>
            {
                if (e.Status is TableDependencyStatus.WaitingForNotification)
                    waiting.TrySetResult();
            };
            dependency.OnExceptionAsync = (ExceptionEventArgs e) => { raisedException = e.Exception; return Task.CompletedTask; };

            var listening = dependency.StartAsync(timeout: 60, watchdogTimeout: 120, waitForStop: true, ct: TestContext.Current.CancellationToken);
            await waiting.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

            // ACT
            // Latch the SIGTERM flag (a real signal would terminate the test host) then sever the connection before the token cancels, exactly as a Kubernetes rollout does.
            dependency._shuttingDown = true;
            await KillSqlTableDependencyDbConnection(TestContext.Current.CancellationToken);

            // ASSERT
            // The drop is a clean stop: no fault surfaced, status reflects a signal-driven shutdown (not a token cancel)
            await listening.WaitAsync(TimeSpan.FromSeconds(90), TestContext.Current.CancellationToken);
            Assert.Null(raisedException);
            Assert.Equal(TableDependencyStatus.StopDueToProcessShutdown, dependency.Status);
        }
        finally
        {
            await dependency.DisposeAsync();
            await dependency.DropDatabaseObjectsAsync();
        }
    }

    [Fact]
    public async Task ShuttingDown_SubscriberFailureDuringNotification()
    {
        // ARRANGE
        var persistentId = $"subfail_{Guid.NewGuid():N}";
        var waiting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var faulted = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriberFailure = new InvalidOperationException("subscriber blew up");

        var dependency = await SqlTableDependency<Model>.CreateSqlTableDependencyAsync(
            DependencyConnectionString,
            tableName: TableName,
            persistentId: persistentId,
            ct: TestContext.Current.CancellationToken);
        dependency.ReconnectTimeout = TimeSpan.Zero; // asserts the immediate stop, not the reconnect

        try
        {
            dependency.OnChanged += _ => throw subscriberFailure;
            dependency.OnStatusChanged += e =>
            {
                if (e.Status is TableDependencyStatus.WaitingForNotification)
                    waiting.TrySetResult();
            };
            dependency.OnExceptionAsync = (ExceptionEventArgs e) => { faulted.TrySetResult(e.Exception); return Task.CompletedTask; };

            await dependency.StartAsync(timeout: 60, watchdogTimeout: 120, ct: TestContext.Current.CancellationToken);
            await waiting.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

            // ACT
            // Shutdown is latched, then a genuine notification arrives whose subscriber throws (not a SQL disconnect)
            dependency._shuttingDown = true;
            await InsertRowAsync(TestContext.Current.CancellationToken);

            // ASSERT
            // The application error is surfaced via OnException, not hidden by the shutdown fast path
            var raisedException = await faulted.Task.WaitAsync(TimeSpan.FromSeconds(90), TestContext.Current.CancellationToken);
            Assert.Same(subscriberFailure, raisedException);
            Assert.Equal(TableDependencyStatus.StopDueToError, dependency.Status);
        }
        finally
        {
            await dependency.DisposeAsync();
            await dependency.DropDatabaseObjectsAsync();
        }
    }

    [Fact]
    public async Task ShuttingDown_HandlerThrowsSqlException_StillReportedAsError()
    {
        // ARRANGE
        var persistentId = $"handlersql_{Guid.NewGuid():N}";
        var waiting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var faulted = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);

        var dependency = await SqlTableDependency<Model>.CreateSqlTableDependencyAsync(
            DependencyConnectionString,
            tableName: TableName,
            persistentId: persistentId,
            ct: TestContext.Current.CancellationToken);
        dependency.ReconnectTimeout = TimeSpan.Zero; // asserts the immediate stop, not the reconnect

        try
        {
            // The subscriber does its own SQL work and that fails - a real application error that is itself a SqlException.
            dependency.OnChangedAsync = async _ =>
            {
                await using var sqlConnection = new SqlConnection(ConnectionString);
                await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);
                await using var sqlCommand = sqlConnection.CreateCommand();
                sqlCommand.CommandText = "RAISERROR('handler-sql-failure', 16, 1);";
                await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            };
            dependency.OnStatusChanged += e =>
            {
                if (e.Status is TableDependencyStatus.WaitingForNotification)
                    waiting.TrySetResult();
            };
            dependency.OnExceptionAsync = (ExceptionEventArgs e) => { faulted.TrySetResult(e.Exception); return Task.CompletedTask; };

            await dependency.StartAsync(timeout: 60, watchdogTimeout: 120, ct: TestContext.Current.CancellationToken);
            await waiting.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

            // ACT
            // Shutdown is latched, then a notification arrives whose handler throws a SqlException (not the severed WAITFOR)
            dependency._shuttingDown = true;
            await InsertRowAsync(TestContext.Current.CancellationToken);

            // ASSERT
            // A handler SqlException is an application error - it must not slip through the shutdown fast path's type check
            var raisedException = await faulted.Task.WaitAsync(TimeSpan.FromSeconds(90), TestContext.Current.CancellationToken);
            Assert.IsType<SqlException>(raisedException);
            Assert.Equal(TableDependencyStatus.StopDueToError, dependency.Status);
        }
        finally
        {
            await dependency.DisposeAsync();
            await dependency.DropDatabaseObjectsAsync();
        }
    }

    [Fact]
    public async Task Restart_AfterStaleShutdownLatch_GenuineOutageStillSurfaces()
    {
        // ARRANGE
        var persistentId = $"restart_{Guid.NewGuid():N}";
        var waiting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Exception? raisedException = null;

        var dependency = await SqlTableDependency<Model>.CreateSqlTableDependencyAsync(
            DependencyConnectionString,
            tableName: TableName,
            persistentId: persistentId,
            ct: TestContext.Current.CancellationToken);
        dependency.ReconnectTimeout = TimeSpan.Zero; // asserts the immediate stop, not the reconnect

        try
        {
            dependency.OnChanged += _ => { };
            dependency.OnStatusChanged += e =>
            {
                if (e.Status is TableDependencyStatus.WaitingForNotification)
                    waiting.TrySetResult();
            };
            dependency.OnExceptionAsync = (ExceptionEventArgs e) => { raisedException = e.Exception; return Task.CompletedTask; };

            // First run, then a graceful stop that leaves a stale latch behind (as an intercepted SIGINT would).
            await dependency.StartAsync(timeout: 60, watchdogTimeout: 120, ct: TestContext.Current.CancellationToken);
            await waiting.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
            await dependency.StopAsync();
            dependency._shuttingDown = true;

            // ACT
            // Restart with a never-cancelled token, then sever the connection: a genuine outage, not a shutdown.
            waiting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var listening = dependency.StartAsync(timeout: 60, watchdogTimeout: 120, waitForStop: true, ct: TestContext.Current.CancellationToken);
            await waiting.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
            await KillSqlTableDependencyDbConnection(TestContext.Current.CancellationToken);

            // ASSERT
            // The stale latch must have been reset by StartAsync, so the outage surfaces as an error rather than being swallowed.
            await listening.WaitAsync(TimeSpan.FromSeconds(90), TestContext.Current.CancellationToken);
            Assert.IsType<SqlException>(raisedException);
            Assert.Equal(TableDependencyStatus.StopDueToError, dependency.Status);
        }
        finally
        {
            await dependency.DisposeAsync();
            await dependency.DropDatabaseObjectsAsync();
        }
    }

    private async Task InsertRowAsync(CancellationToken ct)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(ct);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name]) VALUES ('shutdown-change');";
        await sqlCommand.ExecuteNonQueryAsync(ct);
    }

    private async Task KillSqlTableDependencyDbConnection(CancellationToken ct)
    {
        var initialCatalog = new SqlConnectionStringBuilder(ConnectionString).InitialCatalog;
        // Kill the dependency's login (least-privilege), not the admin login the kill runs under.
        var userId = new SqlConnectionStringBuilder(DependencyConnectionString).UserID;

        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(ct);

        await using var sqlCommand = sqlConnection.CreateCommand();
        // Exclude the current session (@@SPID) so we don't kill our own.
        sqlCommand.CommandText = $"DECLARE @kill varchar(8000); SET @kill = ''; SELECT @kill = @kill + 'kill ' + CONVERT(varchar(5), spid) + ';' FROM master..sysprocesses WHERE dbid = db_id('{initialCatalog}') and loginame = '{userId}' and spid <> @@SPID; EXEC(@kill);";
        await sqlCommand.ExecuteNonQueryAsync(ct);
    }
}