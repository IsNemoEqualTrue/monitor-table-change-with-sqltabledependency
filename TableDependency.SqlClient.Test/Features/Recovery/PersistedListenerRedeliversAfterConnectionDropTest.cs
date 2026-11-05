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

namespace TableDependency.SqlClient.Test.Features.Recovery;

// Pins the consumer-driven recovery contract: library does not reconnect; persisted queue survives so a rebuilt listener drains messages enqueued during the outage.
public sealed class PersistedListenerRedeliversAfterConnectionDropTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private const string TableName = nameof(PersistedListenerRedeliversAfterConnectionDropTest);

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
    public async Task PersistedListener_StopsOnConnectionKill_AndRebuiltListenerDeliversQueuedInsert()
    {
        // ARRANGE
        var persistentId = $"redeliver_{Guid.NewGuid():N}";

        // First listener: start, get into WaitingForNotification, then have its session killed.
        var firstStopped = new TaskCompletionSource<TableDependencyStatus>(TaskCreationOptions.RunContinuationsAsynchronously);
        Exception? firstListenerException = null;

        var firstDependency = await SqlTableDependency<Model>.CreateSqlTableDependencyAsync(
            DependencyConnectionString,
            tableName: TableName,
            persistentId: persistentId,
            ct: TestContext.Current.CancellationToken);
        firstDependency.ReconnectTimeout = TimeSpan.Zero; // asserts the immediate stop, not the reconnect

        try
        {
            // StartAsync requires an OnChanged handler even though this listener is killed before any INSERT.
            firstDependency.OnChanged += _ => { };
            firstDependency.OnStatusChanged += e =>
            {
                if (e.Status is TableDependencyStatus.StopDueToError or TableDependencyStatus.StopDueToCancellation)
                    firstStopped.TrySetResult(e.Status);
            };
            firstDependency.OnExceptionAsync = e => { firstListenerException = e.Exception; return Task.CompletedTask; };

            await firstDependency.StartAsync(timeout: 60, watchdogTimeout: 120, ct: TestContext.Current.CancellationToken);

            await WaitUntilAsync(
                () => firstDependency.Status is TableDependencyStatus.WaitingForNotification,
                TimeSpan.FromSeconds(10),
                TestContext.Current.CancellationToken);

            // ACT
            // Simulate a network blip / failover by killing the listener's session.
            await KillSqlTableDependencyDbConnection(TestContext.Current.CancellationToken);

            var stopReason = await firstStopped.Task.WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
            Assert.Equal(TableDependencyStatus.StopDueToError, stopReason);
            Assert.NotNull(firstListenerException);

            // INSERT while no listener is attached. The persistent queue retains the messages.
            await using var sqlConnection = new SqlConnection(ConnectionString);
            await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

            await using var sqlCommand = sqlConnection.CreateCommand();
            sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name]) VALUES ('after-kill');";
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
        finally
        {
            await firstDependency.DisposeAsync();
        }

        // ASSERT
        // Second listener: same persistentId rebinds to the surviving Service Broker queue and must drain the outage INSERT.
        var delivered = new TaskCompletionSource<Model>(TaskCreationOptions.RunContinuationsAsynchronously);

        var secondDependency = await SqlTableDependency<Model>.CreateSqlTableDependencyAsync(
            DependencyConnectionString,
            tableName: TableName,
            persistentId: persistentId,
            ct: TestContext.Current.CancellationToken);

        try
        {
            secondDependency.OnChanged += e => delivered.TrySetResult(e.Entity);

            await secondDependency.StartAsync(timeout: 60, watchdogTimeout: 120, ct: TestContext.Current.CancellationToken);

            var entity = await delivered.Task.WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
            Assert.Equal("after-kill", entity.Name);
        }
        finally
        {
            await secondDependency.DisposeAsync();
            await secondDependency.DropDatabaseObjectsAsync();
        }
    }

    private static async Task WaitUntilAsync(Func<bool> predicate, TimeSpan timeout, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (predicate())
                return;

            await Task.Delay(TimeSpan.FromMilliseconds(50), ct);
        }

        throw new TimeoutException($"Predicate did not become true within {timeout}.");
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