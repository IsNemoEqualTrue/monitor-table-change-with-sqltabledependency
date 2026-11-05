using Microsoft.Data.SqlClient;
using System.Collections.Concurrent;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.Health;
using TableDependency.SqlClient.Exceptions;
using TableDependency.SqlClient.Test.Support;

namespace TableDependency.SqlClient.Test.Features.Recovery;

// Own database: disabling the broker needs exclusive access and would break every test sharing the fixture database.
public sealed class BrokerSelfHealingTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private const string TableName = nameof(BrokerSelfHealingTest);

    private readonly string _database = $"TD_Broker_{Guid.NewGuid():N}";
    private readonly string _ownerLogin = $"td_brk_own_{Guid.NewGuid():N}"[..30];
    private readonly string _limitedLogin = $"td_brk_lim_{Guid.NewGuid():N}"[..30];
    private string _adminConnectionString = string.Empty;

    private sealed class Model
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public override async ValueTask InitializeAsync()
    {
        await ExecuteOnMasterAsync($"CREATE DATABASE [{_database}]; ALTER DATABASE [{_database}] SET ENABLE_BROKER;");
        _adminConnectionString = new SqlConnectionStringBuilder(ConnectionString) { InitialCatalog = _database, Pooling = false }.ConnectionString; // a pooled idle admin session would block the NO_WAIT enable
        await ExecuteAsync($"CREATE TABLE [{TableName}] ([Id] INT IDENTITY(1, 1) NOT NULL PRIMARY KEY, [Name] NVARCHAR(100) NOT NULL);");
    }

    public override async ValueTask DisposeAsync()
    {
        SqlConnection.ClearAllPools();
        foreach (var database in (string[])[_database, CopyDatabase])
            await ExecuteOnMasterAsync($"IF DB_ID(N'{database}') IS NOT NULL BEGIN ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{database}]; END");
        await ExecuteOnMasterAsync($"IF SUSER_ID(N'{_ownerLogin}') IS NOT NULL DROP LOGIN [{_ownerLogin}]; IF SUSER_ID(N'{_limitedLogin}') IS NOT NULL DROP LOGIN [{_limitedLogin}];");
    }

    private string CopyDatabase => $"{_database}_copy";

    [Fact]
    public async Task CheckDatabaseObjects_ReportsDisabledBroker_WhenObjectsAreIntact()
    {
        // ARRANGE
        var connectionString = await SqlServerPrincipalFactory.CreateLoginAsync(
            _adminConnectionString, _ownerLogin, [.. SqlServerPrincipalFactory.LegacyDatabasePermissions, "CONTROL"], [], TestContext.Current.CancellationToken);
        var (dependency, _, _) = await StartListenerAsync(connectionString);
        await using var __ = dependency;
        dependency.AutoRepair = false;

        // ACT
        // DISABLE_BROKER leaves every object in place; only the broker flag changes.
        await ExecuteOnMasterAsync($"ALTER DATABASE [{_database}] SET DISABLE_BROKER WITH ROLLBACK IMMEDIATE;");
        var health = await dependency.CheckDatabaseObjectsAsync(TestContext.Current.CancellationToken);

        // ASSERT
        var issue = Assert.Single(health.Issues);
        Assert.Equal(DatabaseObjectKind.Broker, issue.Kind);
        Assert.Equal(DatabaseObjectProblem.Disabled, issue.Problem);
        Assert.Equal(health.Issues, health.Remaining);
        Assert.False(health.IsHealthy);
    }

    [Fact]
    public async Task Listener_FallsBackToNewBroker_WhenRestoredBrokerIdIsAlreadyInUse()
    {
        // ARRANGE
        var connectionString = await SqlServerPrincipalFactory.CreateLoginAsync(
            _adminConnectionString, _ownerLogin, [.. SqlServerPrincipalFactory.LegacyDatabasePermissions, "CONTROL"], [], TestContext.Current.CancellationToken);
        var (dependency, delivered, statuses) = await StartListenerAsync(connectionString);
        await using var _ = dependency;
        var originalBrokerGuid = await GetBrokerGuidAsync(_database);
        await ExecuteOnMasterAsync($"BACKUP DATABASE [{_database}] TO DISK = N'{_database}.bak' WITH INIT, COPY_ONLY;");

        // ACT
        // Login off first, so a reconnect attempt cannot take the single-user slot the restore needs.
        await ExecuteOnMasterAsync($"ALTER LOGIN [{_ownerLogin}] DISABLE;");
        await ExecuteOnMasterAsync($"ALTER DATABASE [{_database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;" +
            $" RESTORE DATABASE [{_database}] FROM DISK = N'{_database}.bak' WITH REPLACE;" +
            $" ALTER DATABASE [{_database}] SET MULTI_USER;");

        // A second restore holds the same broker ID, enabled, so ENABLE_BROKER on the original now raises 9772.
        await ExecuteOnMasterAsync($"RESTORE DATABASE [{CopyDatabase}] FROM DISK = N'{_database}.bak' WITH {await BuildMoveClauseAsync()}, ENABLE_BROKER;");
        await ExecuteOnMasterAsync($"ALTER LOGIN [{_ownerLogin}] ENABLE;");

        var recovered = await PollUntilAsync(async () => await IsBrokerEnabledAsync() && dependency.Status is TableDependencyStatus.WaitingForNotification,
            TimeSpan.FromSeconds(120), TestContext.Current.CancellationToken);
        await ExecuteAsync($"INSERT INTO [{TableName}] ([Name]) VALUES (N'after-new-broker');");

        // ASSERT
        Assert.True(recovered, $"Statuses: [{string.Join(", ", statuses)}]; broker={await IsBrokerEnabledAsync()}");
        Assert.Equal(originalBrokerGuid, await GetBrokerGuidAsync(CopyDatabase));
        Assert.NotEqual(originalBrokerGuid, await GetBrokerGuidAsync(_database));
        Assert.True(await PollUntilAsync(() => Task.FromResult(delivered.Contains("after-new-broker")), TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken));
        Assert.DoesNotContain(TableDependencyStatus.StopDueToError, statuses);
    }

    [Fact]
    public async Task Listener_ReEnablesBrokerItIsPermittedToAlter_AndDeliversNextInsert()
    {
        // ARRANGE
        var connectionString = await SqlServerPrincipalFactory.CreateLoginAsync(
            _adminConnectionString, _ownerLogin, [.. SqlServerPrincipalFactory.LegacyDatabasePermissions, "CONTROL"], [], TestContext.Current.CancellationToken);
        var (dependency, delivered, statuses) = await StartListenerAsync(connectionString);
        await using var _ = dependency;

        // ACT
        // A restore or revert leaves the broker off; ROLLBACK IMMEDIATE also severs the listener, as the revert would.
        await ExecuteOnMasterAsync($"ALTER DATABASE [{_database}] SET DISABLE_BROKER WITH ROLLBACK IMMEDIATE;");
        var recovered = await PollUntilAsync(async () => await IsBrokerEnabledAsync() && dependency.Status is TableDependencyStatus.WaitingForNotification,
            TimeSpan.FromSeconds(90), TestContext.Current.CancellationToken);
        await ExecuteAsync($"INSERT INTO [{TableName}] ([Name]) VALUES (N'after-broker');");

        // ASSERT
        Assert.True(recovered, $"Statuses: [{string.Join(", ", statuses)}]; broker={await IsBrokerEnabledAsync()}");
        Assert.True(await PollUntilAsync(() => Task.FromResult(delivered.Contains("after-broker")), TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken));
        Assert.DoesNotContain(TableDependencyStatus.StopDueToError, statuses);
        Assert.True((await dependency.CheckDatabaseObjectsAsync(TestContext.Current.CancellationToken)).IsHealthy);
    }

    [Fact]
    public async Task CheckDatabaseObjects_ReportsRepairError_WhenBrokerCannotBeEnabled()
    {
        // ARRANGE
        var brokerSchema = BrokerSchemaName;
        var connectionString = await SqlServerPrincipalFactory.CreateLoginAsync(
            _adminConnectionString, _limitedLogin, SqlServerPrincipalFactory.BrokerCreatePermissions,
            [SqlServerPrincipalFactory.CreateSchemaStatement(brokerSchema, _limitedLogin)], TestContext.Current.CancellationToken);
        await SqlServerPrincipalFactory.GrantTableObjectPermissionsAsync(_adminConnectionString, _limitedLogin, "dbo", TableName, TestContext.Current.CancellationToken);
        var (dependency, _, _) = await StartListenerAsync(connectionString);
        await using var __ = dependency;
        await ExecuteOnMasterAsync($"ALTER DATABASE [{_database}] SET DISABLE_BROKER WITH ROLLBACK IMMEDIATE;");

        // ACT
        var health = await dependency.CheckDatabaseObjectsAsync(TestContext.Current.CancellationToken);

        // ASSERT
        Assert.False(health.IsHealthy);
        Assert.Contains(health.Remaining, i => i is { Kind: DatabaseObjectKind.Broker, Problem: DatabaseObjectProblem.Disabled });
        Assert.IsType<ServiceBrokerNotEnabledException>(health.RepairError);
        Assert.False(await IsBrokerEnabledAsync());
    }

    private async Task<(SqlTableDependency<Model>, ConcurrentQueue<string>, ConcurrentQueue<TableDependencyStatus>)> StartListenerAsync(string connectionString)
    {
        var dependency = await SqlTableDependency<Model>.CreateSqlTableDependencyAsync(
            connectionString, tableName: TableName, persistentId: "broker", ct: TestContext.Current.CancellationToken);

        ConcurrentQueue<string> delivered = new();
        ConcurrentQueue<TableDependencyStatus> statuses = new();
        dependency.OnChanged += e => delivered.Enqueue(e.Entity.Name);
        dependency.OnStatusChanged += e => statuses.Enqueue(e.Status);

        await dependency.StartAsync(timeout: 60, watchdogTimeout: 120, ct: TestContext.Current.CancellationToken);
        await PollUntilAsync(() => Task.FromResult(dependency.Status is TableDependencyStatus.WaitingForNotification), TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
        return (dependency, delivered, statuses);
    }

    private async Task<Guid> GetBrokerGuidAsync(string database)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);
        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"SELECT service_broker_guid FROM sys.databases WHERE name = N'{database}';";
        return (Guid)(await sqlCommand.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
    }

    // The copy needs its own files: same logical names, physical paths suffixed so they do not collide with the original's.
    private async Task<string> BuildMoveClauseAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);
        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"SELECT name, physical_name FROM sys.master_files WHERE database_id = DB_ID(N'{_database}');";

        List<string> moves = [];
        await using var reader = await sqlCommand.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            var physical = reader.GetString(1);
            var extension = Path.GetExtension(physical);
            moves.Add($"MOVE N'{reader.GetString(0)}' TO N'{physical[..^extension.Length]}_copy{extension}'");
        }

        return string.Join(", ", moves);
    }

    private async Task<bool> IsBrokerEnabledAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);
        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"SELECT is_broker_enabled FROM sys.databases WHERE name = N'{_database}';";
        return (bool)(await sqlCommand.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var sqlConnection = new SqlConnection(_adminConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);
        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = sql;
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
    }

    private async Task ExecuteOnMasterAsync(string sql)
    {
        await using var sqlConnection = new SqlConnection(new SqlConnectionStringBuilder(ConnectionString) { InitialCatalog = "master", Pooling = false }.ConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);
        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = sql;
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
    }
}