using Microsoft.Data.SqlClient;
using System.Collections.Concurrent;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.Health;
using TableDependency.SqlClient.Test.Support;

namespace TableDependency.SqlClient.Test.Features.Recovery;

// Own login so kills and login disables never touch the sessions of tests running in parallel.
public sealed class SelfHealingTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private const string TableName = nameof(SelfHealingTest);
    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(30);

    private readonly string _login = $"td_selfheal_{Guid.NewGuid():N}"[..30];
    private string _connectionString = string.Empty;

    private sealed class Model
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private sealed class Listener : IAsyncDisposable
    {
        public required SqlTableDependency<Model> Dependency { get; init; }
        public ConcurrentQueue<string> Delivered { get; } = new();
        public ConcurrentQueue<TableDependencyStatus> Statuses { get; } = new();
        public ConcurrentQueue<Exception?> Exceptions { get; } = new();

        public async ValueTask DisposeAsync()
        {
            await Dependency.DisposeAsync();
            await Dependency.DropDatabaseObjectsAsync();
        }
    }

    public override async ValueTask InitializeAsync()
    {
        _connectionString = await SqlServerPrincipalFactory.CreateLoginAsync(
            ConnectionString,
            _login,
            [.. SqlServerPrincipalFactory.LegacyDatabasePermissions, "CONTROL"],
            extraStatements: [],
            TestContext.Current.CancellationToken);

        await ExecuteAsync($"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];" +
            $" CREATE TABLE [{TableName}] ([Id] INT IDENTITY(1, 1) NOT NULL PRIMARY KEY, [Name] NVARCHAR(100) NOT NULL);");
    }

    public override async ValueTask DisposeAsync()
    {
        await ExecuteAsync($"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];");
        await ExecuteAsync($"ALTER LOGIN [{_login}] ENABLE;");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Listener_ReconnectsAfterSessionKill_AndDeliversNextInsert(bool persisted)
    {
        // ARRANGE
        await using var listener = await StartListenerAsync(persisted);

        // ACT
        await KillListenerSessionsAsync();
        await WaitForStatusAfterAsync(listener, TableDependencyStatus.Reconnecting, TableDependencyStatus.WaitingForNotification);
        await InsertAsync("after-kill");

        // ASSERT
        Assert.True(await WaitForDeliveryAsync(listener, "after-kill"), Describe(listener));
        Assert.DoesNotContain(TableDependencyStatus.StopDueToError, listener.Statuses);
        Assert.Empty(listener.Exceptions);
    }

    [Fact]
    public async Task Listener_RebuildsObjectsDroppedDuringOutage()
    {
        // ARRANGE
        await using var listener = await StartListenerAsync(persisted: true);
        var naming = listener.Dependency.NamingPrefix;

        // ACT
        // Snapshot-revert stand-in: the session dies and the objects are gone before the listener can reconnect.
        await ExecuteAsync($"ALTER LOGIN [{_login}] DISABLE;");
        await KillListenerSessionsAsync();
        await DropBrokerObjectsAsync(naming);
        await ExecuteAsync($"ALTER LOGIN [{_login}] ENABLE;");

        await WaitForStatusAfterAsync(listener, TableDependencyStatus.Reconnecting, TableDependencyStatus.WaitingForNotification);
        await InsertAsync("after-revert");

        // ASSERT
        Assert.True(await WaitForDeliveryAsync(listener, "after-revert"), Describe(listener));
        Assert.DoesNotContain(TableDependencyStatus.StopDueToError, listener.Statuses);
    }

    [Fact]
    public async Task Listener_StopsWithError_AfterReconnectTimeout()
    {
        // ARRANGE
        await using var listener = await StartListenerAsync(persisted: true, reconnectTimeout: TimeSpan.FromSeconds(3));

        // ACT
        await ExecuteAsync($"ALTER LOGIN [{_login}] DISABLE;");
        await KillListenerSessionsAsync();
        var stopped = await PollUntilAsync(() => Task.FromResult(listener.Dependency.Status is TableDependencyStatus.StopDueToError), TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        await ExecuteAsync($"ALTER LOGIN [{_login}] ENABLE;");

        // ASSERT
        Assert.True(stopped, Describe(listener));
        Assert.Contains(TableDependencyStatus.Reconnecting, listener.Statuses);
        Assert.Single(listener.Exceptions);
    }

    [Fact]
    public async Task Listener_SubscriberSqlFault_StillStopsWithError()
    {
        // ARRANGE
        await using var listener = await StartListenerAsync(persisted: true);
        listener.Dependency.OnChanged += _ =>
        {
            using var connection = new SqlConnection(ConnectionString);
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT 1 / 0;";
            command.ExecuteScalar();
        };

        // ACT
        await InsertAsync("poison");
        var stopped = await PollUntilAsync(() => Task.FromResult(listener.Dependency.Status is TableDependencyStatus.StopDueToError), TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

        // ASSERT
        Assert.True(stopped, Describe(listener));
        Assert.DoesNotContain(TableDependencyStatus.Reconnecting, listener.Statuses);
        Assert.IsType<SqlException>(Assert.Single(listener.Exceptions));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task CheckDatabaseObjects_RebuildsDroppedTrigger_AndNextInsertIsDelivered(bool persisted)
    {
        // ARRANGE
        await using var listener = await StartListenerAsync(persisted);
        var naming = listener.Dependency.NamingPrefix;
        await ExecuteAsync($"DROP TRIGGER [dbo].[tr_{naming}_Sender];");

        // ACT
        var health = await listener.Dependency.CheckDatabaseObjectsAsync(TestContext.Current.CancellationToken);
        await InsertAsync("after-repair");

        // ASSERT
        Assert.Contains(new DatabaseObjectIssue(DatabaseObjectKind.Trigger, $"tr_{naming}_Sender", DatabaseObjectProblem.Missing), health.Issues);
        Assert.Empty(health.Remaining);
        Assert.True(health.IsHealthy, string.Join(", ", health.Remaining) + health.RepairError);
        Assert.Same(health, listener.Dependency.LastObjectsHealth);
        Assert.True(await WaitForDeliveryAsync(listener, "after-repair"), Describe(listener));
    }

    [Fact]
    public async Task CheckDatabaseObjects_ReEnablesDisabledTrigger()
    {
        // ARRANGE
        await using var listener = await StartListenerAsync(persisted: true);
        var naming = listener.Dependency.NamingPrefix;
        await ExecuteAsync($"DISABLE TRIGGER [dbo].[tr_{naming}_Sender] ON [dbo].[{TableName}];");

        // ACT
        var health = await listener.Dependency.CheckDatabaseObjectsAsync(TestContext.Current.CancellationToken);
        await InsertAsync("after-enable");

        // ASSERT
        Assert.Contains(new DatabaseObjectIssue(DatabaseObjectKind.Trigger, $"tr_{naming}_Sender", DatabaseObjectProblem.Disabled), health.Issues);
        Assert.True(health.IsHealthy, string.Join(", ", health.Remaining) + health.RepairError);
        Assert.True(await WaitForDeliveryAsync(listener, "after-enable"), Describe(listener));
    }

    [Fact]
    public async Task CheckDatabaseObjects_RebuildsDroppedService_WriterNoLongerFails()
    {
        // ARRANGE
        await using var listener = await StartListenerAsync(persisted: true);
        var naming = listener.Dependency.NamingPrefix;
        await ExecuteAsync($"DROP SERVICE [{naming}_Receiver];");
        var writerFailedBeforeRepair = await Record.ExceptionAsync(() => InsertAsync("before-repair")) is SqlException;

        // ACT
        var health = await listener.Dependency.CheckDatabaseObjectsAsync(TestContext.Current.CancellationToken);
        await InsertAsync("after-repair");

        // ASSERT
        Assert.True(writerFailedBeforeRepair);
        Assert.Contains(new DatabaseObjectIssue(DatabaseObjectKind.Service, $"{naming}_Receiver", DatabaseObjectProblem.Missing), health.Issues);
        Assert.True(health.IsHealthy, string.Join(", ", health.Remaining) + health.RepairError);
        Assert.True(await WaitForDeliveryAsync(listener, "after-repair"), Describe(listener));
    }

    [Fact]
    public async Task CheckDatabaseObjects_ReEnablesDisabledQueueAndActivation()
    {
        // ARRANGE
        await using var listener = await StartListenerAsync(persisted: true);
        var naming = listener.Dependency.NamingPrefix;
        await ExecuteAsync($"ALTER QUEUE [{BrokerSchemaName}].[{naming}_Sender] WITH STATUS = OFF, ACTIVATION (STATUS = OFF);");

        // ACT
        var health = await listener.Dependency.CheckDatabaseObjectsAsync(TestContext.Current.CancellationToken);

        // ASSERT
        Assert.Contains(new DatabaseObjectIssue(DatabaseObjectKind.Queue, $"{naming}_Sender", DatabaseObjectProblem.Disabled), health.Issues);
        Assert.Contains(new DatabaseObjectIssue(DatabaseObjectKind.QueueActivation, $"{naming}_Sender", DatabaseObjectProblem.Disabled), health.Issues);
        Assert.True(health.IsHealthy, string.Join(", ", health.Remaining) + health.RepairError);
    }

    [Fact]
    public async Task CheckDatabaseObjects_AutoRepairOff_ReportsWithoutRepairing()
    {
        // ARRANGE
        await using var listener = await StartListenerAsync(persisted: true);
        listener.Dependency.AutoRepair = false;
        var naming = listener.Dependency.NamingPrefix;
        await ExecuteAsync($"DROP TRIGGER [dbo].[tr_{naming}_Sender];");

        // ACT
        var health = await listener.Dependency.CheckDatabaseObjectsAsync(TestContext.Current.CancellationToken);

        // ASSERT
        Assert.False(health.IsHealthy);
        Assert.Equal(health.Issues, health.Remaining);
        Assert.False(await TriggerExistsAsync(naming));
    }

    [Fact]
    public async Task CheckDatabaseObjects_AfterStop_DoesNotResurrectObjects()
    {
        // ARRANGE
        await using var listener = await StartListenerAsync(persisted: false);
        var naming = listener.Dependency.NamingPrefix;
        await listener.Dependency.StopAsync();

        // ACT
        var health = await listener.Dependency.CheckDatabaseObjectsAsync(TestContext.Current.CancellationToken);

        // ASSERT
        Assert.False(health.Listening);
        Assert.NotEmpty(health.Remaining);
        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task CheckDatabaseObjects_ConcurrentCalls_AllEndHealthy()
    {
        // ARRANGE
        await using var listener = await StartListenerAsync(persisted: false);
        var naming = listener.Dependency.NamingPrefix;
        await ExecuteAsync($"DROP TRIGGER [dbo].[tr_{naming}_Sender];");

        // ACT
        var results = await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => listener.Dependency.CheckDatabaseObjectsAsync(TestContext.Current.CancellationToken)));
        await InsertAsync("after-concurrent");

        // ASSERT
        Assert.All(results, r => Assert.True(r.IsHealthy, string.Join(", ", r.Remaining) + r.RepairError));
        Assert.Single(results, r => r.Issues.Count > 0);
        Assert.True(await WaitForDeliveryAsync(listener, "after-concurrent"), Describe(listener));
        Assert.Equal(1, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private async Task<Listener> StartListenerAsync(bool persisted, TimeSpan? reconnectTimeout = null)
    {
        var dependency = await SqlTableDependency<Model>.CreateSqlTableDependencyAsync(
            _connectionString,
            tableName: TableName,
            persistentId: persisted ? $"heal_{Guid.NewGuid():N}" : null,
            ct: TestContext.Current.CancellationToken);

        var listener = new Listener { Dependency = dependency };
        dependency.ReconnectTimeout = reconnectTimeout ?? TimeSpan.FromMinutes(5);
        dependency.OnChanged += e => listener.Delivered.Enqueue(e.Entity.Name);
        dependency.OnStatusChanged += e => listener.Statuses.Enqueue(e.Status);
        dependency.OnException += e => listener.Exceptions.Enqueue(e.Exception);

        await dependency.StartAsync(timeout: 60, watchdogTimeout: 120, ct: TestContext.Current.CancellationToken);
        await PollUntilAsync(() => Task.FromResult(dependency.Status is TableDependencyStatus.WaitingForNotification), TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
        return listener;
    }

    private static async Task WaitForStatusAfterAsync(Listener listener, TableDependencyStatus first, TableDependencyStatus then)
    {
        var reached = await PollUntilAsync(() =>
        {
            var statuses = listener.Statuses.ToArray();
            var index = Array.IndexOf(statuses, first);
            return Task.FromResult(index >= 0 && Array.IndexOf(statuses, then, index) > index);
        }, TimeSpan.FromSeconds(60), TestContext.Current.CancellationToken);

        Assert.True(reached, Describe(listener));
    }

    private static Task<bool> WaitForDeliveryAsync(Listener listener, string name)
        => PollUntilAsync(() => Task.FromResult(listener.Delivered.Contains(name)), DeliveryTimeout, TestContext.Current.CancellationToken);

    private static string Describe(Listener listener)
        => $"Statuses: [{string.Join(", ", listener.Statuses)}]; status={listener.Dependency.Status}; exceptions: [{string.Join(" | ", listener.Exceptions.Select(e => e?.Message))}]";

    private Task InsertAsync(string name)
        => ExecuteAsync($"INSERT INTO [{TableName}] ([Name]) VALUES (N'{name}');");

    private async Task<bool> TriggerExistsAsync(string naming)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);
        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"SELECT COUNT(*) FROM sys.triggers WHERE name = N'tr_{naming}_Sender';";
        return (int)(await sqlCommand.ExecuteScalarAsync(TestContext.Current.CancellationToken))! > 0;
    }

    private Task KillListenerSessionsAsync()
        => ExecuteAsync($"DECLARE @kill NVARCHAR(MAX) = N''; SELECT @kill += N'KILL ' + CONVERT(NVARCHAR(10), session_id) + N';' FROM sys.dm_exec_sessions WHERE login_name = N'{_login}' AND session_id <> @@SPID; EXEC(@kill);");

    private Task DropBrokerObjectsAsync(string naming)
        => ExecuteAsync($"DROP TRIGGER [dbo].[tr_{naming}_Sender];" +
            $" DROP SERVICE [{naming}_Receiver]; DROP SERVICE [{naming}_Sender];" +
            $" DROP QUEUE [{BrokerSchemaName}].[{naming}_Receiver]; DROP QUEUE [{BrokerSchemaName}].[{naming}_Sender];" +
            $" DROP PROCEDURE [{BrokerSchemaName}].[{naming}_QueueActivationSender]; DROP CONTRACT [{naming}];");

    private async Task ExecuteAsync(string sql)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);
        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = sql;
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
    }
}