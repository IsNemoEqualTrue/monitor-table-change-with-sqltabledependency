// aislop-ignore-file complexity/file-too-large -- Partial extraction would fragment the cohesive listener implementation and its private state.
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
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Diagnostics;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;
using TableDependency.SqlClient.Base.Exceptions;
using TableDependency.SqlClient.Base.Health;
using TableDependency.SqlClient.Base.Interfaces;
using TableDependency.SqlClient.Base.Messages;
using TableDependency.SqlClient.Base.Utilities;
using TableDependency.SqlClient.Enums;
using TableDependency.SqlClient.Exceptions;
using TableDependency.SqlClient.Extensions;
using TableDependency.SqlClient.Messages;
using TableDependency.SqlClient.Resources;

namespace TableDependency.SqlClient;

// aislop-ignore-next-block complexity/function-too-long -- This type coordinates the listener lifecycle and deliberately owns its state.
public sealed class SqlTableDependency<T> : ITableDependency<T> where T : class, new()
{
    #region Public Constants

    public const string ActivitySourceName = nameof(SqlTableDependency<>);

    /// <summary>
    /// Dedicated schema holding the Service Broker objects (queues + activation procedure); the trigger stays on the monitored table's schema.
    /// Create it owned by the application principal (<c>CREATE SCHEMA [SqlTableDependency] AUTHORIZATION [app_user]</c>)
    /// to run without database-wide CONTROL. If it is missing, the library creates it on startup (needs CREATE SCHEMA);
    /// when it cannot be created a <see cref="Exceptions.BrokerSchemaUnavailableException"/> is thrown.
    /// </summary>
    public const string DefaultBrokerSchemaName = "SqlTableDependency";

    #endregion

    #region Private Variables

    private static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    private readonly string _connectionString;
    private readonly string _server;
    private readonly string _database;

    private IList<string> _updateOf = [];
    private IEnumerable<TableColumnInfo> _userInterestedColumns = [];
    private TableColumnInfo[] _tableColumns = [];
    private IList<string> _keyColumns = [];
    private NotifyOn _notifyOn;
    private ITableDependencyFilter? _filter;

    private readonly ILogger? _logger;
    private readonly bool _includeOldEntity;

    // Swapped by repair while the receive loop reads it; a fresh box per write so a reader never sees a torn Guid.
    private volatile StrongBox<Guid> _conversationHandle = new(Guid.Empty);

    // Serialises check/repair calls with each other, with the loop's reconnect repair, and with the non-persisted final drop.
    private readonly SemaphoreSlim _repairLock = new(1, 1);

    // True from the start of the receive loop until its final drop; repair never runs outside it, so it cannot resurrect stopped objects.
    private volatile bool _loopActive;
    private volatile CancellationTokenSource? _receiveCts;
    private volatile TaskCompletionSource? _receiveClosed;
    private int _watchdogTimeout;
    private TimeSpan _reconnectTimeout = TimeSpan.FromMinutes(30);
    private const string _startMessageTemplate = "{0}/StartMessage/{1}";
    private const string _endMessageTemplate = "{0}/EndMessage";
    private readonly Regex _sqlAllowedChars = new(@"^[a-zA-Z]\w*(?: \w+)*$");
    private CancellationTokenSource? _cancellationTokenSource;
    private Task? _task;
    internal volatile bool _shuttingDown;
    private PosixSignalRegistration[] _shutdownSignalRegistrations = [];
    private string[] _processableMessages = [];
    private readonly bool _persisted;
    private readonly bool _isExpando;
    private bool _supportOldTypes;

    #endregion

    #region Interface Events

    public event Action<ExceptionEventArgs>? OnException;
    public Func<ExceptionEventArgs, Task>? OnExceptionAsync { get; set; }

    public event Action<StatusChangedEventArgs>? OnStatusChanged;
    public Func<StatusChangedEventArgs, Task>? OnStatusChangedAsync { get; set; }

    public event Action<RecordChangedEventArgs<T>>? OnChanged;
    public Func<RecordChangedEventArgs<T>, Task>? OnChangedAsync { get; set; }

    #endregion

    #region Interface Properties


    public bool ShuttingDown => _shuttingDown;
    public CultureInfo CultureInfo { get; set; } = CultureInfo.CurrentCulture;
    public Encoding Encoding { get; set; } = Encoding.Unicode;
    public string NamingPrefix { get; }
    public TableDependencyStatus Status { get; private set; }
    public string SchemaName { get; }
    public string TableName { get; }

    #endregion

    #region Properties

    /// <summary>
    /// Gets or sets a value indicating whether activate database logging and event viewer logging.
    /// </summary>
    /// <remarks>
    /// Only a member of the sysadmin fixed server role or a user with ALTER TRACE permissions can use it.
    /// </remarks>
    /// <value>
    /// <c>true</c> if [activate database logging]; otherwise, <c>false</c>.
    /// </value>
    public bool ActivateDatabaseLogging { get; set; }

    /// <summary>
    /// Specifies the owner of the service to the specified database user.
    /// When a new service is created it is owned by the principal specified in the AUTHORIZATION clause. Server, database, and schema names cannot be specified. The service_name must be a valid sysname.
    /// When the current user is dbo or sa, owner_name may be the name of any valid user or role.
    /// Otherwise, owner_name must be the name of the current user, the name of a user that the current user has IMPERSONATE permission for, or the name of a role to which the current user belongs.
    /// </summary>
    public string? ServiceAuthorization { get; set; }

    /// <summary>
    /// Specifies the SQL Server database user account under which the activation stored procedure runs.
    /// SQL Server must be able to check the permissions for this user at the time that the queue activates the stored procedure. For aWindows domain user, the server must be connected to the domain
    /// when the procedure is activated or when activation fails.For a SQL Server user, Service Broker always checks the permissions.EXECUTE AS SELF means that the stored procedure executes as the current user.
    /// </summary>
    public string QueueExecuteAs { get; set; } = "SELF";

    /// <summary>
    /// Gets the ModelToTableMapper.
    /// </summary>
    public IModelToTableMapper<T>? Mapper { get; private set; }

    /// <summary>
    /// When true (the default), <see cref="CheckDatabaseObjectsAsync"/> and the reconnect loop rebuild missing or disabled objects while the listener runs.
    /// When false they only report.
    /// </summary>
    public bool AutoRepair { get; set; } = true;

    /// <summary>
    /// How long the receive loop keeps reconnecting after it loses its connection or its objects before it stops with
    /// <see cref="TableDependencyStatus.StopDueToError"/>. Default 30 minutes; <see cref="TimeSpan.Zero"/> stops at once (the pre-10.2 behaviour);
    /// <see cref="Timeout.InfiniteTimeSpan"/> never gives up.
    /// </summary>
    public TimeSpan ReconnectTimeout
    {
        get => _reconnectTimeout;
        set => _reconnectTimeout = value >= TimeSpan.Zero || value == Timeout.InfiniteTimeSpan
            ? value
            : throw new ArgumentOutOfRangeException(nameof(value), value, "ReconnectTimeout must be zero, positive or Timeout.InfiniteTimeSpan.");
    }

    public DatabaseObjectsHealth? LastObjectsHealth { get; private set; }

    private Guid ConversationHandle
    {
        get => _conversationHandle.Value;
        set => _conversationHandle = new(value);
    }

    #endregion

    #region Constructors

    /// <summary>
    /// SqlTableDependency class: monitor SQL Server table record changes and notify it.
    /// </summary>
    /// <remarks>
    /// Initializes a new instance of the <see cref="SqlTableDependency{T}" /> class.
    /// </remarks>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="schemaName">Name of the schema.</param>
    /// <param name="tableName">Name of the table.</param>
    /// <param name="mapper">The model to database table column mapper.</param>
    /// <param name="updateOf">List of columns that need to monitor for changing on order to receive notifications.</param>
    /// <param name="notifyOn">The notify on Insert, Delete, Update operation.</param>
    /// <param name="filter">The filter condition translated in WHERE.</param>
    /// <param name="logger">Logger to write out to</param>
    /// <param name="includeOldEntity">if set to <c>true</c>, include old entity.</param>
    /// <param name="persistentId">An id to append to the naming convention that enables queue persistence on restart.</param>
    /// <param name="ct">Cancellation token.</param>
    // aislop-ignore-next-block complexity/too-many-params -- Preserve the established named-argument construction API.
    public static async Task<SqlTableDependency<T>> CreateSqlTableDependencyAsync(
        string connectionString,
        string? schemaName = null,
        string? tableName = null,
        IModelToTableMapper<T>? mapper = null,
        IUpdateOfModel<T>? updateOf = null,
        NotifyOn notifyOn = NotifyOn.All,
        ITableDependencyFilter? filter = null,
        ILogger? logger = null,
        bool includeOldEntity = false,
        string? persistentId = null,
        CancellationToken ct = default)
    {
        SqlTableDependency<T> obj = new(connectionString, schemaName, tableName, logger, includeOldEntity, persistentId);
        await obj.ConfigureAsync(mapper, updateOf, notifyOn, filter, ct);
        return obj;
    }

    private SqlTableDependency(string connectionString, string? schemaName, string? tableName, ILogger? logger, bool includeOldEntity, string? persistentId)
    {
        _connectionString = connectionString;
        _server = new SqlConnectionStringBuilder(_connectionString).DataSource;
        _database = new SqlConnectionStringBuilder(_connectionString).InitialCatalog;

        SchemaName = GetSchemaName(schemaName);
        TableName = GetTableName(tableName);

        _logger = logger;
        _includeOldEntity = includeOldEntity;

        persistentId = string.IsNullOrWhiteSpace(persistentId) ? null : persistentId.Trim();
        _persisted = persistentId is not null;
        _isExpando = typeof(T) == typeof(ExpandoObject);

        NamingPrefix = $"{SchemaName}_{TableName}_{persistentId ?? Guid.NewGuid().ToString()}";
    }

    // Latch shutdown on a termination signal (it precedes the token cancel and socket teardown)
    private PosixSignalRegistration[] RegisterShutdownSignals()
    {
        PosixSignal[] signals = [PosixSignal.SIGTERM, PosixSignal.SIGINT, PosixSignal.SIGQUIT];
        List<PosixSignalRegistration> registrations = [];

        foreach (var signal in signals)
        {
            try
            {
                registrations.Add(PosixSignalRegistration.Create(signal, _ => _shuttingDown = true));
            }
            catch (Exception exception) when (exception is PlatformNotSupportedException or ArgumentException)
            {
                // Signal unavailable on this platform
            }
        }

        return [.. registrations];
    }

    private async Task ConfigureAsync(
        IModelToTableMapper<T>? mapper,
        IUpdateOfModel<T>? updateOf,
        NotifyOn notifyOn,
        ITableDependencyFilter? filter,
        CancellationToken ct)
    {
        using var activity = StartActivity(nameof(ConfigureAsync));

        // If we're outputting expando event args, we don't need mappers
        if (_isExpando)
        {
            mapper = null;
            updateOf = null;
        }

        if (mapper?.Mappings.Count is 0)
            throw new UpdateOfException("mapper parameter is empty.");
        if (updateOf?.PropertyInfo.Count is 0)
            throw new UpdateOfException("updateOf parameter is empty.");

        await _connectionString.TestConnectionAsync(ct);

        var sqlVersion = await _connectionString.GetSqlServerVersionAsync(ct);
        if (sqlVersion < SqlServerVersion.SqlServer2008)
            throw new SqlServerVersionNotSupportedException(sqlVersion);

        activity?.AddTagAndBaggage("db.version", sqlVersion.ToString());

        await _connectionString.CheckServiceBrokerIsEnabledAsync(ct);
        await _connectionString.CheckTableExistsAsync(SchemaName, TableName, ct);

        // Create the dedicated broker schema if it does not exist, then check perms against the real securables.
        await _connectionString.EnsureBrokerSchemaExistsAsync(DefaultBrokerSchemaName, ct);
        await _connectionString.CheckUserHasPermissionsAsync(SchemaName, TableName, DefaultBrokerSchemaName, ct);

        var tableColumns = await _connectionString.GetTableColumnsAsync(SchemaName, TableName, ct);
        if (tableColumns.Length is 0)
            throw new TableWithNoColumnsException(TableName);

        _tableColumns = tableColumns;
        activity?.AddTag("tabledependency.tableColumns", _tableColumns.Length);

        Mapper = mapper ?? tableColumns.CreateModelMapperFromColumnDataAnnotation<T>();
        CheckMapperValidity();

        CheckUpdateOfWithTriggerType(updateOf, notifyOn);
        _updateOf = GetUpdateOfColumnNameList(updateOf);

        _userInterestedColumns = _isExpando ? _tableColumns : GetUserInterestedColumns();
        activity?.AddTag("tabledependency.userInterestedColumns", _userInterestedColumns.Count());
        if (!_userInterestedColumns.Any())
            throw new NoMatchBetweenModelAndTableColumns();

        _supportOldTypes = _userInterestedColumns.Any(c => c.IsLegacyTextOrImage());
        activity?.AddTag("tabledependency.supportOldTypes", _supportOldTypes);
        if (_supportOldTypes)
        {
            _keyColumns = await GetKeyColumnsListAsync(ct);
            if (_keyColumns.Count is 0)
                throw new TableWithNoKeyException(TableName);
        }

        _notifyOn = notifyOn;
        _filter = filter;
    }

    #endregion

    #region Public Methods

    public async Task StartAsync(int timeout = 120, int watchdogTimeout = 180, bool waitForStop = false, CancellationToken ct = default)
    {
        using var activity = StartActivity(nameof(StartAsync))
            ?.SetTag("tabledependency.timeout", timeout)
            .SetTag("tabledependency.watchdogTimeout", watchdogTimeout);

        if (timeout < 60)
            throw new ArgumentException("timeout must be greater or equal to 60 seconds");

        if (watchdogTimeout < 60 || watchdogTimeout < (timeout + 60))
            throw new WatchdogTimeoutException("watchdogTimeout must be at least 60 seconds bigger than timeout");

        if (_task is not null)
            throw new AlreadyListeningException();

        if (OnChanged is null && OnChangedAsync is null)
            throw new NoSubscriberException();

        await NotifyListenersAboutStatus(TableDependencyStatus.Starting);

        _processableMessages = [.. BuildProcessableMessagesList()];
        _watchdogTimeout = watchdogTimeout;
        await CreateDatabaseObjectsAsync(watchdogTimeout, repair: false, ct);
        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _shuttingDown = false;
        _shutdownSignalRegistrations = RegisterShutdownSignals();

        LogDebug("Starting wait for notifications.");
        _task = WaitForNotificationsAsync(timeout, watchdogTimeout, _cancellationTokenSource.Token);

        LogInformation("Waiting for receiving {TableName}'s records change notifications.", ("TableName", TableName));
        if (waitForStop)
            await _task;
    }

    public async Task StopAsync()
    {
        using var activity = StartActivity(nameof(StopAsync));

        try
        {
            if (_task is not null)
            {
                _cancellationTokenSource?.Cancel(true);
                await _task;
            }

            _task = null;
        }
        finally
        {
        // Release the signal handlers so they stop rooting this instance once it is no longer listening.
        foreach (var registration in _shutdownSignalRegistrations)
                registration.Dispose();
            _shutdownSignalRegistrations = [];
        }

        LogInformation("Stopped waiting for notification.");
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _cancellationTokenSource?.Dispose();

        OnException = null;
        OnExceptionAsync = null;
        OnStatusChanged = null;
        OnStatusChangedAsync = null;
        OnChanged = null;
        OnChangedAsync = null;

        LogInformation("Disposed table dependency.");
    }

    public async Task<DatabaseObjectsHealth> CheckDatabaseObjectsAsync(CancellationToken ct = default)
    {
        using var activity = StartActivity(nameof(CheckDatabaseObjectsAsync));

        await _repairLock.WaitAsync(ct);
        try
        {
            DatabaseObjectsHealth health;
            try
            {
                health = await CheckAndRepairCoreAsync(ct);
            }
            catch (Exception exception) when (exception is not OperationCanceledException || !ct.IsCancellationRequested)
            {
                // A heartbeat must get an answer during the very outage it exists to report, not an exception it may not catch.
                LogWarning(exception, "Database objects check for {NamingPrefix} could not run.", (nameof(NamingPrefix), NamingPrefix));
                health = new DatabaseObjectsHealth(DateTimeOffset.UtcNow, Status is TableDependencyStatus.WaitingForNotification, [], [], exception);
                LastObjectsHealth = health;
            }

            activity?.SetTag("tabledependency.objects.healthy", health.IsHealthy)
                .SetTag("tabledependency.objects.issues", health.Issues.Count)
                .SetTag("tabledependency.objects.remaining", health.Remaining.Count);

            return health;
        }
        finally
        {
            _repairLock.Release();
        }
    }

    #endregion

    #region Health

    // Caller holds _repairLock.
    private async Task<DatabaseObjectsHealth> CheckAndRepairCoreAsync(CancellationToken ct)
    {
        var issues = await SelectDatabaseObjectIssuesAsync(ct);
        var remaining = issues;
        Exception? repairError = null;

        if (issues.Count > 0)
        {
            LogWarning(null, "Database objects {NamingPrefix} unhealthy: {Issues}.", (nameof(NamingPrefix), NamingPrefix), ("Issues", string.Join(", ", issues)));

            // Never repair outside a running loop: after StopAsync the objects were dropped on purpose.
            if (AutoRepair && _loopActive)
            {
                // Broker first: NEW_BROKER ends every conversation, so the object repair below must see that and begin a fresh dialog.
                if (issues.Any(i => i.Kind is DatabaseObjectKind.Broker))
                    repairError = await TryEnableBrokerAsync(ct);

                // DDL works with the broker off, so the objects are rebuilt even when it could not be enabled.
                try
                {
                    await CreateDatabaseObjectsAsync(_watchdogTimeout, repair: true, ct);
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    repairError ??= exception;
                    LogError(exception, "Repair of database objects {NamingPrefix} failed.", (nameof(NamingPrefix), NamingPrefix));
                }

                remaining = await SelectDatabaseObjectIssuesAsync(ct);
                LogInformation("Database objects {NamingPrefix} repaired: {Issues}.", (nameof(NamingPrefix), NamingPrefix), ("Issues", string.Join(", ", issues.Except(remaining))));
            }
        }

        var health = new DatabaseObjectsHealth(DateTimeOffset.UtcNow, Status is TableDependencyStatus.WaitingForNotification, issues, remaining, repairError);
        LastObjectsHealth = health;
        return health;
    }

    // Best effort; returns why the broker is still off, or null once enabled.
    private async Task<Exception?> TryEnableBrokerAsync(CancellationToken ct)
    {
        try
        {
            if (!await _connectionString.HasDatabasePermissionAsync("ALTER", ct))
            {
                LogWarning(null, "Service Broker is disabled on {Database} and this principal lacks ALTER on the database to enable it.", ("Database", _database));
                return new ServiceBrokerNotEnabledException(_database);
            }

            // The listener's own WAITFOR session is "another user" to ALTER DATABASE ... NO_WAIT, so let it go first.
            await ReleaseReceiveConnectionAsync(ct);

            // Enabling needs exclusive access, so release our own idle pooled sessions before asking for it.
            await using (var pooled = new SqlConnection(_connectionString))
                SqlConnection.ClearPool(pooled);

            await using var sqlConnection = new SqlConnection(_connectionString);
            await sqlConnection.OpenAsync(ct);
            await using var sqlCommand = sqlConnection.CreateCommand();
            sqlCommand.CommandText = SqlScripts.EnableBroker;
            sqlCommand.CommandTimeout = 30;

            if (await sqlCommand.ExecuteScalarAsync(ct) is not 1)
            {
                LogWarning(null, "Service Broker is disabled on {Database} and this principal lacks ALTER on the database to enable it.", ("Database", _database));
                return new ServiceBrokerNotEnabledException(_database);
            }

            LogInformation("Service Broker enabled on {Database}.", ("Database", _database));
            return null;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            LogError(exception, "Enabling Service Broker on {Database} failed.", ("Database", _database));
            return exception;
        }
    }

    private async Task<List<DatabaseObjectIssue>> SelectDatabaseObjectIssuesAsync(CancellationToken ct)
    {
        await using var sqlConnection = new SqlConnection(_connectionString);
        await sqlConnection.OpenAsync(ct);

        var handle = ConversationHandle;
        await using var sqlCommand = PrepareCommand(sqlConnection.CreateCommand(), SqlScripts.SelectDatabaseObjectIssues,
            ("@brokerSchema", DefaultBrokerSchemaName),
            ("@tableSchema", SchemaName),
            ("@trigger", $"tr_{NamingPrefix}_Sender"),
            ("@receiver", $"{NamingPrefix}_Receiver"),
            ("@sender", $"{NamingPrefix}_Sender"),
            ("@contract", NamingPrefix),
            ("@messages", BuildContractMessagesXml()),
            ("@procedure", $"{NamingPrefix}_QueueActivationSender"),
            ("@handle", _persisted || handle == Guid.Empty ? null : handle));

        List<DatabaseObjectIssue> issues = [];
        await using var reader = await sqlCommand.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            issues.Add(new(
                Enum.Parse<DatabaseObjectKind>(reader.GetString(0)),
                reader.GetString(1),
                Enum.Parse<DatabaseObjectProblem>(reader.GetString(2))));
        }

        return issues;
    }

    private string BuildContractMessagesXml()
        => new XElement("m", _processableMessages.Select(message => new XElement("m", message))).ToString(SaveOptions.DisableFormatting);

    #endregion

    #region Configure Methods

    private string GetSchemaName(string? schemaName)
    {
        schemaName = schemaName?.Replace("[", string.Empty).Replace("]", string.Empty);
        if (!string.IsNullOrWhiteSpace(schemaName) && _sqlAllowedChars.IsMatch(schemaName))
            return schemaName;

        var schemaNameFromDataAnnotation = typeof(T).GetCustomAttribute<TableAttribute>()?.Schema;
        return !string.IsNullOrWhiteSpace(schemaNameFromDataAnnotation)
            ? schemaNameFromDataAnnotation
            : "dbo";
    }

    private string GetTableName(string? tableName)
    {
        tableName = tableName?.Replace("[", string.Empty).Replace("]", string.Empty);
        if (!string.IsNullOrWhiteSpace(tableName) && _sqlAllowedChars.IsMatch(tableName))
            return tableName;

        var tableNameFromDataAnnotation = typeof(T).GetCustomAttribute<TableAttribute>()?.Name;
        return !string.IsNullOrWhiteSpace(tableNameFromDataAnnotation)
            ? tableNameFromDataAnnotation
            : typeof(T).Name;
    }

    private void CheckMapperValidity()
    {
        if (Mapper?.Mappings.Count is null or < 1)
            return;

        var dbColumnNames = _tableColumns.Select(t => t.Name.ToLowerInvariant()).ToArray();

        if (Mapper.Mappings
            .Select(t => t.Value)
            .Any(mappingColumnName => !dbColumnNames.Contains(mappingColumnName.ToLowerInvariant())))
            throw new ModelToTableMapperException("I cannot find any correspondence between defined ModelToTableMapper properties and database Table columns.");
    }

    private static void CheckUpdateOfWithTriggerType(IUpdateOfModel<T>? updateOf, NotifyOn notifyOn)
    {
        if (notifyOn is 0)
            throw new NotifyOnException("notifyOn cannot be 0");

        if (updateOf?.PropertyInfo.Count is null or 0)
            return;

        if (!notifyOn.HasFlag(NotifyOn.Update) && !notifyOn.HasFlag(NotifyOn.All) && updateOf.PropertyInfo.Count > 0)
            throw new NotifyOnException("updateOf parameter can be specified only if NotifyOn parameter contains NotifyOn.Update too, not for NotifyOn.Delete or NotifyOn.Insert only.");
    }

    private List<string> GetUpdateOfColumnNameList(IUpdateOfModel<T>? updateOf)
    {
        var updateOfList = new List<string>();

        if (updateOf?.PropertyInfo.Count is null or <= 0)
            return updateOfList;

        foreach (var propertyInfo in updateOf.PropertyInfo)
        {
            var existingMap = Mapper?.GetMapping(propertyInfo);
            if (existingMap is not null)
            {
                updateOfList.Add(existingMap);
                continue;
            }

            var attribute = propertyInfo.GetCustomAttribute<ColumnAttribute>();
            if (attribute is not null)
            {
                var dbColumnName = attribute.Name;
                if (!string.IsNullOrWhiteSpace(dbColumnName))
                {
                    updateOfList.Add(dbColumnName);
                    continue;
                }

                var entityPropertyInfo = ModelUtil.GetModelPropertiesInfo<T>().First(mpf => mpf.Name == propertyInfo.Name);
                var propertyMappedTo = Mapper?.GetMapping(entityPropertyInfo);
                var propertyName = propertyMappedTo ?? entityPropertyInfo.Name;

                // If model property is mapped to table column keep it
                var tableColumn = _tableColumns.FirstOrDefault(tableColumn => tableColumn.Name.Equals(propertyName, StringComparison.OrdinalIgnoreCase));

                updateOfList.Add(tableColumn?.Name ?? propertyInfo.Name);
                continue;
            }

            updateOfList.Add(propertyInfo.Name);
        }

        return updateOfList;
    }

    private List<TableColumnInfo> GetUserInterestedColumns()
    {
        var filteredTableColumns = new List<TableColumnInfo>();

        foreach (var entityPropertyInfo in ModelUtil.GetModelPropertiesInfo<T>())
        {
            var notMappedAttribute = entityPropertyInfo.GetCustomAttribute<NotMappedAttribute>();
            if (notMappedAttribute is not null)
                continue;

            var propertyMappedTo = Mapper?.GetMapping(entityPropertyInfo);
            var propertyName = propertyMappedTo ?? entityPropertyInfo.Name;

            // If model property is mapped to table column keep it
            foreach (var tableColumn in _tableColumns.Where(c => c.Name.Equals(propertyName, StringComparison.InvariantCultureIgnoreCase)))
            {
                if (filteredTableColumns.Any(ci => ci.Name.Equals(tableColumn.Name, StringComparison.OrdinalIgnoreCase)))
                    throw new ModelToTableMapperException("Your model specify a [Column] attributed Name that has same name of another model property.");

                filteredTableColumns.Add(tableColumn);
            }
        }

        return filteredTableColumns;
    }

    private async Task<IList<string>> GetKeyColumnsListAsync(CancellationToken ct)
    {
        var keyColumns = new List<string>();

        await using var sqlConnection = new SqlConnection(_connectionString);
        await sqlConnection.OpenAsync(ct);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = string.Format(SqlScripts.TableKeyColumns, SchemaName, TableName);

        string? selectedIndex = null;
        await using var reader = await sqlCommand.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var indexName = reader["INDEX_NAME"].ToString();
            if (string.IsNullOrWhiteSpace(indexName))
                continue;

            if (selectedIndex is null)
                selectedIndex = indexName;

            if (!string.Equals(selectedIndex, indexName, StringComparison.OrdinalIgnoreCase))
                break;

            var columnName = reader["COLUMN_NAME"].ToString();
            if (!string.IsNullOrWhiteSpace(columnName))
                keyColumns.Add(columnName);
        }

        return keyColumns;
    }

    #endregion

    #region Start Methods

    private List<string> BuildProcessableMessagesList()
    {
        var messages = new List<string>()
        {
            string.Format(_startMessageTemplate, NamingPrefix, ChangeType.Insert),
            string.Format(_startMessageTemplate, NamingPrefix, ChangeType.Update),
            string.Format(_startMessageTemplate, NamingPrefix, ChangeType.Delete)
        };

        foreach (var columnName in _userInterestedColumns.Select(c => c.Name))
        {
            messages.Add($"{NamingPrefix}/{columnName}");
            if (_includeOldEntity)
                messages.Add($"{NamingPrefix}/{columnName}/old");
        }

        messages.Add(string.Format(_endMessageTemplate, NamingPrefix));
        return messages;
    }

    // aislop-ignore-next-block complexity/function-too-long -- The ordered DDL transaction must remain inspectable as one unit.
    // Every step is idempotent, so repair re-runs the whole script; it differs only in tolerating existing objects and reusing a live dialog.
    private async Task CreateDatabaseObjectsAsync(int watchdogTimeout, bool repair, CancellationToken ct)
    {
        using var activity = StartActivity(nameof(CreateDatabaseObjectsAsync))
            ?.SetTag("tabledependency.watchdogTimeout", watchdogTimeout)
            .SetTag("tabledependency.repair", repair);

        if (!repair)
        {
            var dbObjectsExist = await _connectionString.CheckIfDatabaseObjectsExistAsync(NamingPrefix, ct);
            activity?.SetTag("tabledependency.dbObjectsExist", dbObjectsExist);
            if (dbObjectsExist && !_persisted)
                throw new DbObjectsWithSameNameException(NamingPrefix);
        }

        await using var sqlConnection = new SqlConnection(_connectionString);
        await sqlConnection.OpenAsync(ct);

        await using var transaction = await sqlConnection.BeginTransactionAsync(ct);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.Transaction = (SqlTransaction)transaction;

        // Messages
        foreach (var message in _processableMessages)
        {
            await PrepareCommand(sqlCommand, SqlScripts.CreateMessageType, ("@message", message)).ExecuteNonQueryAsync(ct);
            LogDebug("Message {Message} created.", ("Message", message));
        }

        // Contract
        await PrepareCommand(sqlCommand, SqlScripts.CreateContract, ("@contract", NamingPrefix), ("@messages", BuildContractMessagesXml())).ExecuteNonQueryAsync(ct);
        LogDebug("Contract {NamingPrefix} created.", (nameof(NamingPrefix), NamingPrefix));

        // Queues (broker schema)
        await PrepareCommand(sqlCommand, SqlScripts.CreateQueue, ("@schema", DefaultBrokerSchemaName), ("@queue", $"{NamingPrefix}_Receiver")).ExecuteNonQueryAsync(ct);
        LogDebug("Queue {NamingPrefix}_Receiver created.", (nameof(NamingPrefix), NamingPrefix));

        await PrepareCommand(sqlCommand, SqlScripts.CreateQueue, ("@schema", DefaultBrokerSchemaName), ("@queue", $"{NamingPrefix}_Sender")).ExecuteNonQueryAsync(ct);
        LogDebug("Queue {NamingPrefix}_Sender created.", (nameof(NamingPrefix), NamingPrefix));

        foreach (var queue in (string[])[$"{NamingPrefix}_Receiver", $"{NamingPrefix}_Sender"])
            await PrepareCommand(sqlCommand, SqlScripts.EnableQueue, ("@schema", DefaultBrokerSchemaName), ("@queue", queue)).ExecuteNonQueryAsync(ct);

        // Services
        var authorization = string.IsNullOrWhiteSpace(ServiceAuthorization) ? null : ServiceAuthorization;
        await PrepareCommand(sqlCommand, SqlScripts.CreateService,
            ("@service", $"{NamingPrefix}_Sender"),
            ("@authorization", authorization),
            ("@schema", DefaultBrokerSchemaName),
            ("@queue", $"{NamingPrefix}_Sender"),
            ("@contract", null)).ExecuteNonQueryAsync(ct);
        LogDebug("Service broker {NamingPrefix}_Sender created.", (nameof(NamingPrefix), NamingPrefix));

        await PrepareCommand(sqlCommand, SqlScripts.CreateService,
            ("@service", $"{NamingPrefix}_Receiver"),
            ("@authorization", authorization),
            ("@schema", DefaultBrokerSchemaName),
            ("@queue", $"{NamingPrefix}_Receiver"),
            ("@contract", NamingPrefix)).ExecuteNonQueryAsync(ct);
        LogDebug("Service broker {NamingPrefix}_Receiver created.", (nameof(NamingPrefix), NamingPrefix));

        // Activation Store Procedure
        var dropMessages = string.Join(Environment.NewLine, _processableMessages.Select((pm, index) => index > 0
            ? (Spacer(8) + string.Format("IF EXISTS (SELECT * FROM sys.service_message_types WITH (NOLOCK) WHERE name = N'{0}') DROP MESSAGE TYPE [{0}];", pm))
            : string.Format("IF EXISTS (SELECT * FROM sys.service_message_types WITH (NOLOCK) WHERE name = N'{0}') DROP MESSAGE TYPE [{0}];", pm)));

        // If persisted mode is enabled, don't include drop-all logic in the activation procedure
        var dropAllScript = _persisted ? string.Empty : PrepareScriptDropAll(dropMessages, false);

        await PrepareCommand(sqlCommand, PrepareScriptProcedureQueueActivation(dropAllScript)).ExecuteNonQueryAsync(ct);
        LogDebug("Procedure {NamingPrefix} created.", (nameof(NamingPrefix), NamingPrefix));

        // Begin conversation. The field is only swapped after commit, so a rolled-back repair leaves the loop on the old handle.
        var conversationHandle = ConversationHandle;
        if (!_persisted)
        {
            var reuse = repair && conversationHandle != Guid.Empty
                && await PrepareCommand(sqlCommand, SqlScripts.ConversationIsOpen, ("@handle", conversationHandle)).ExecuteScalarAsync(ct) is 1;

            if (!reuse)
            {
                // A replaced dialog left alive would later fire its DialogTimer, whose activation runs the non-persisted drop-all.
                if (conversationHandle != Guid.Empty)
                    await PrepareCommand(sqlCommand, SqlScripts.EndConversationWithCleanup, ("@handle", conversationHandle)).ExecuteNonQueryAsync(ct);

                conversationHandle = await BeginConversationAsync(sqlCommand, ct);
                LogDebug("Conversation with handler {ConversationHandle} started.", ("ConversationHandle", conversationHandle.ToString()));
            }
        }

        // Trigger
        await CreateTriggerAsync(sqlCommand, conversationHandle, ct);
        await PrepareCommand(sqlCommand, SqlScripts.EnableTrigger,
            ("@schema", SchemaName),
            ("@trigger", $"tr_{NamingPrefix}_Sender"),
            ("@table", TableName)).ExecuteNonQueryAsync(ct);

        // Associate Activation Store Procedure to sender queue. Quoted names stay distinct from SELF and OWNER keywords.
        var (executeAs, executeAsIsQuoted) = NormalizeQueueExecuteAs(QueueExecuteAs);
        await PrepareCommand(sqlCommand, SqlScripts.ActivateQueue,
            ("@schema", DefaultBrokerSchemaName),
            ("@queue", $"{NamingPrefix}_Sender"),
            ("@procedure", $"{NamingPrefix}_QueueActivationSender"),
            ("@executeAs", executeAs),
            ("@executeAsIsQuoted", executeAsIsQuoted)).ExecuteNonQueryAsync(ct);
        LogDebug("Associated Activation Store Procedure to sender queue.");

        // Run the watch-dog
        if (!_persisted)
        {
            await PrepareCommand(sqlCommand, SqlScripts.BeginConversationTimer, ("@handle", conversationHandle), ("@timeout", watchdogTimeout)).ExecuteNonQueryAsync(ct);
            LogDebug("Watch dog started.");
        }

        await transaction.CommitAsync(ct);
        if (!_persisted)
            ConversationHandle = conversationHandle;
        LogInformation("All OK! Database objects ensured with naming {NamingPrefix}.", (nameof(NamingPrefix), NamingPrefix));

        if (_persisted)
            await ReconnectToPersistedDatabaseObjectsAsync(ct);
    }

    private async Task ReconnectToPersistedDatabaseObjectsAsync(CancellationToken ct)
    {
        using var activity = StartActivity(nameof(ReconnectToPersistedDatabaseObjectsAsync));

        await using var sqlConnection = new SqlConnection(_connectionString);
        await sqlConnection.OpenAsync(ct);

        await using var sqlCommand = PrepareCommand(sqlConnection.CreateCommand(), SqlScripts.SelectActiveInitiatorConversation,
            ("@farService", $"{NamingPrefix}_Receiver"),
            ("@localService", $"{NamingPrefix}_Sender"));

        ConversationHandle = await sqlCommand.ExecuteScalarAsync(ct) is Guid handle
            ? handle
            : await BeginConversationAsync(sqlCommand, ct);

        LogInformation("Using persisted database objects with naming {NamingPrefix}.", (nameof(NamingPrefix), NamingPrefix));
    }

    private async Task CreateTriggerAsync(SqlCommand sqlCommand, Guid conversationHandle, CancellationToken ct)
    {
        sqlCommand.Parameters.Clear();

        var declareVariableStatement = PrepareDeclareVariableStatement();
        var selectForSetVariablesStatement = PrepareSelectForSetVariables();
        var sendInsertConversationStatements = PrepareSendConversation(ChangeType.Insert);
        var sendUpdatedConversationStatements = PrepareSendConversation(ChangeType.Update);
        var sendDeletedConversationStatements = PrepareSendConversation(ChangeType.Delete);

        var columnsForUpdateOf = _updateOf is null
            ? null
            : string.Join(" OR ", _updateOf.Where(c => !string.IsNullOrWhiteSpace(c)).Distinct(StringComparer.CurrentCultureIgnoreCase).Select(c => $"UPDATE([{c}])"));

        var columnsForExceptTable = PrepareColumnListForTableVariable();
        var columnsForDeletedTable = PrepareColumnListForTableVariable();
        var columnsForModifiedRecordsTable = PrepareColumnListForTableVariable(_includeOldEntity);
        var triggerTiming = _supportOldTypes ? "INSTEAD OF" : "AFTER";
        var insertDmlScript = _supportOldTypes ? PrepareInsteadOfInsertScript() : string.Empty;
        var updateDmlScript = _supportOldTypes ? PrepareInsteadOfUpdateScript() : string.Empty;
        var deleteDmlScript = _supportOldTypes ? PrepareInsteadOfDeleteScript() : string.Empty;

        // If persisted, reuse any active conversation or start a new one; otherwise drop objects and stop.
        var conversationSetupScript = _persisted
            ? $"DECLARE @conversationHandle UNIQUEIDENTIFIER{Environment.NewLine}"
              + $"SELECT TOP(1) @conversationHandle = conversation_handle{Environment.NewLine}"
              + "FROM sys.conversation_endpoints WITH (NOLOCK)" + Environment.NewLine
              + $"WHERE far_service = N'{NamingPrefix}_Receiver' AND is_initiator = 1 AND state_desc NOT IN ('CLOSED', 'ERROR')"
              + $" AND service_id = (SELECT service_id FROM sys.services WITH (NOLOCK) WHERE name = N'{NamingPrefix}_Sender');{Environment.NewLine}"
              + "IF @conversationHandle IS NULL" + Environment.NewLine
              + "BEGIN" + Environment.NewLine
              + $"    BEGIN DIALOG CONVERSATION @conversationHandle FROM SERVICE [{NamingPrefix}_Sender] TO SERVICE '{NamingPrefix}_Receiver' ON CONTRACT [{NamingPrefix}] WITH ENCRYPTION = OFF;{Environment.NewLine}"
              + "END"
            : $"DECLARE @conversationHandle UNIQUEIDENTIFIER = '{conversationHandle}';{Environment.NewLine}"
              + "IF NOT EXISTS (SELECT 1 FROM sys.conversation_endpoints WHERE conversation_handle = @conversationHandle)" + Environment.NewLine
              + "BEGIN" + Environment.NewLine
              + PrepareScriptDropAll(string.Empty, true) + Environment.NewLine
              + "RETURN;" + Environment.NewLine
              + "END";

        if (_supportOldTypes)
            conversationSetupScript = $"IF TRIGGER_NESTLEVEL() > 1 RETURN;{Environment.NewLine}{conversationSetupScript}";

        sqlCommand.CommandText = string.Format(
            SqlScripts.CreateTrigger,
            NamingPrefix,
            $"[{SchemaName}].[{TableName}]",
            columnsForModifiedRecordsTable,
            PrepareColumnListForSelectFromTableVariable(),
            PrepareInsertIntoTableVariableForUpdateChange(columnsForUpdateOf, updateDmlScript),
            declareVariableStatement,
            selectForSetVariablesStatement,
            sendInsertConversationStatements,
            sendUpdatedConversationStatements,
            sendDeletedConversationStatements,
            ChangeType.Insert,
            ChangeType.Update,
            ChangeType.Delete,
            string.Join(", ", _notifyOn.GetDmlTriggerTypes()),
            CreateWhereCondition(),
            PrepareTriggerLogScript(),
            ActivateDatabaseLogging ? " WITH LOG" : string.Empty,
            columnsForExceptTable,
            columnsForDeletedTable,
            conversationSetupScript,
            triggerTiming,
            deleteDmlScript,
            insertDmlScript);

        await sqlCommand.ExecuteNonQueryAsync(ct);
        LogDebug("Trigger {NamingPrefix} created.", (nameof(NamingPrefix), NamingPrefix));
    }

    private string PrepareDeclareVariableStatement()
    {
        var declarations = _userInterestedColumns.Select(c =>
        {
            var variableName = SanitizeVariableName(c.Name);
            var variableType = c.GetVariableSqlType();

            var declare = $"DECLARE {variableName} {variableType.ToLowerInvariant()}";
            if (_includeOldEntity)
                declare += $", {variableName}_old {variableType.ToLowerInvariant()}";

            return declare;
        });

        return string.Join(Environment.NewLine + Spacer(4), declarations);
    }

    private string PrepareSelectForSetVariables()
    {
        var result = string.Join(", ", _userInterestedColumns.Select(interestedColumn => $"{SanitizeVariableName(interestedColumn.Name)} = [{interestedColumn.Name}]"));
        if (_includeOldEntity)
            result += ", " + string.Join(", ", _userInterestedColumns.Select(interestedColumn => $"{SanitizeVariableName(interestedColumn.Name)}_old = [{interestedColumn.Name}_old]"));

        return result;
    }

    private string PrepareSendConversation(ChangeType dmlType)
    {
        List<string> sendList = [$";SEND ON CONVERSATION @conversationHandle MESSAGE TYPE [{string.Format(_startMessageTemplate, NamingPrefix, dmlType)}] (CONVERT(NVARCHAR, @dmlType)){Environment.NewLine}"];

        sendList.AddRange(_userInterestedColumns
            .Select(c =>
            {
                var sendStatement = $"{Spacer(16)}IF {SanitizeVariableName(c.Name)} IS NOT NULL BEGIN"
                    + $"{Environment.NewLine}{Spacer(20)};SEND ON CONVERSATION @conversationHandle MESSAGE TYPE [{NamingPrefix}/{c.Name}] ({ConvertValueByType(c)})"
                    + $"{Environment.NewLine}{Spacer(16)}END"
                    + $"{Environment.NewLine}{Spacer(16)}ELSE BEGIN"
                    + $"{Environment.NewLine}{Spacer(20)};SEND ON CONVERSATION @conversationHandle MESSAGE TYPE [{NamingPrefix}/{c.Name}] (0x)"
                    + $"{Environment.NewLine}{Spacer(16)}END";

                if (_includeOldEntity)
                    sendStatement += $"{Environment.NewLine}{Spacer(16)}IF {SanitizeVariableName(c.Name)}_old IS NOT NULL BEGIN"
                        + $"{Environment.NewLine}{Spacer(20)};SEND ON CONVERSATION @conversationHandle MESSAGE TYPE [{NamingPrefix}/{c.Name}/old] ({ConvertValueByType(c, _includeOldEntity)})"
                        + $"{Environment.NewLine}{Spacer(16)}END"
                        + $"{Environment.NewLine}{Spacer(16)}ELSE BEGIN"
                        + $"{Environment.NewLine}{Spacer(20)};SEND ON CONVERSATION @conversationHandle MESSAGE TYPE [{NamingPrefix}/{c.Name}/old] (0x)"
                        + $"{Environment.NewLine}{Spacer(16)}END";

                return sendStatement;
            }));

        sendList.Add($"{Environment.NewLine + Spacer(16)};SEND ON CONVERSATION @conversationHandle MESSAGE TYPE [{string.Format(_endMessageTemplate, NamingPrefix)}] (0x)");

        return string.Join(Environment.NewLine, sendList);
    }

    private string SanitizeVariableName(string tableColumnName)
        => $"@var{_userInterestedColumns.Index().First(c => c.Item.Name == tableColumnName).Index + 1}";

    private string PrepareColumnListForTableVariable(bool includeOldEntity = false)
    {
        var columns = _userInterestedColumns.Select(c =>
        {
            var typeToUse = c.GetTableVariableSqlType();

            if (includeOldEntity)
                return $"[{c.Name}] {typeToUse}, [{c.Name}_old] {typeToUse}";
            return $"[{c.Name}] {typeToUse}";
        });

        return string.Join(", ", columns);
    }

    private string PrepareInsteadOfInsertScript()
    {
        var insertColumns = GetDmlAssignableColumns();
        if (insertColumns.Length is 0)
            return string.Empty;

        var columnList = string.Join(", ", insertColumns.Select(c => $"[{c.Name}]"));
        var selectList = string.Join(", ", insertColumns.Select(c => $"[{c.Name}]"));

        return $"INSERT INTO [{SchemaName}].[{TableName}] ({columnList}) SELECT {selectList} FROM INSERTED;";
    }

    private string PrepareInsteadOfUpdateScript()
    {
        var updateColumns = GetDmlAssignableColumns();
        if (updateColumns.Length is 0)
            return string.Empty;

        var setClause = string.Join(", ", updateColumns.Select(c => $"t.[{c.Name}] = i.[{c.Name}]"));
        return $"UPDATE t SET {setClause} FROM [{SchemaName}].[{TableName}] t INNER JOIN INSERTED i ON {PrepareKeyJoinCondition("t", "i")};";
    }

    private string PrepareInsteadOfDeleteScript()
        => $"DELETE t FROM [{SchemaName}].[{TableName}] t INNER JOIN DELETED d ON {PrepareKeyJoinCondition("t", "d")};";

    private TableColumnInfo[] GetDmlAssignableColumns()
        => [.. _tableColumns.Where(c => !c.IsComputed && !c.IsIdentity && !c.IsRowVersionOrTimestamp())];

    private string PrepareKeyJoinCondition(string leftAlias, string rightAlias)
        => string.Join(" AND ", _keyColumns.Select(c => $"{leftAlias}.[{c}] = {rightAlias}.[{c}]"));

    private string ConvertValueByType(TableColumnInfo userInterestedColumn, bool isOld = false)
    {
        var oldNameExtension = isOld ? "_old" : string.Empty;

        return userInterestedColumn.Type.ToLowerInvariant() switch
        {
            "binary" or "varbinary" or "timestamp" or "image" => SanitizeVariableName(userInterestedColumn.Name) + oldNameExtension,
            "float" => $"CONVERT(NVARCHAR(MAX), RTRIM(LTRIM(STR({SanitizeVariableName(userInterestedColumn.Name)}{oldNameExtension}{ConvertFormat(userInterestedColumn)}, 53, 16))))",
            _ => $"CONVERT(NVARCHAR(MAX), {SanitizeVariableName(userInterestedColumn.Name)}{oldNameExtension}{ConvertFormat(userInterestedColumn)})"
        };
    }

    private static string ConvertFormat(TableColumnInfo userInterestedColumn)
        => userInterestedColumn.Type.ToLowerInvariant() is "datetime" or "date"
            ? ", 121"
            : string.Empty;

    private string PrepareColumnListForSelectFromTableVariable()
        => string.Join(", ", _userInterestedColumns.Select(c =>
        {
            var columnValue = c.PrepareColumnValueForInsertedDeletedTable();
            return _includeOldEntity ? $"{columnValue}, NULL" : columnValue;
        }));

    private string PrepareInsertIntoTableVariableForUpdateChange(string? columnsForUpdateOf, string? updateDmlScript)
    {
        var comma = new Separator(2, ",");
        var sBuilderColumns = new StringBuilder();

        foreach (var column in _userInterestedColumns)
            sBuilderColumns.Append(comma.PopSeparator() + column.PrepareColumnValueForInsertedDeletedTable());

        var insertedAndDeletedTableVariable =
            $"INSERT INTO @deletedTable SELECT {sBuilderColumns} FROM DELETED{Environment.NewLine}"
            + $"{Spacer(12)}INSERT INTO @insertedTable SELECT {sBuilderColumns} FROM INSERTED{Environment.NewLine}";

        if (!string.IsNullOrWhiteSpace(updateDmlScript))
            insertedAndDeletedTableVariable += $"{Spacer(12)}{updateDmlScript}{Environment.NewLine}";

        string insertIntoExceptStatement = _userInterestedColumns.Any(tableColumn => tableColumn.IsRowVersionOrTimestamp())
            ? $"{insertedAndDeletedTableVariable}{Spacer(12)}INSERT INTO @exceptTable SELECT [RowNumber],{sBuilderColumns} FROM @insertedTable"
            : $"{insertedAndDeletedTableVariable}{Spacer(12)}INSERT INTO @exceptTable SELECT [RowNumber],{sBuilderColumns} FROM @insertedTable EXCEPT SELECT [RowNumber],{sBuilderColumns} FROM @deletedTable";

        if (_includeOldEntity)
        {
            comma = new Separator(2, ",");
            sBuilderColumns = new StringBuilder();
            foreach (var columnName in _userInterestedColumns.Select(c => c.Name))
            {
                sBuilderColumns.Append($"{comma.PopSeparator()}[{columnName}]");
                sBuilderColumns.Append($"{comma.PopSeparator()}(SELECT d.[{columnName}] FROM @deletedTable d WHERE d.[RowNumber] = e.[RowNumber])");
            }
        }

        var whereCondition = CreateWhereCondition();

        var insertIntoExceptTableStatement = $"{insertIntoExceptStatement}{Environment.NewLine}{Environment.NewLine}"
            + $"{Spacer(12)}INSERT INTO @modifiedRecordsTable SELECT {sBuilderColumns} FROM @exceptTable e {whereCondition}";

        return !string.IsNullOrEmpty(columnsForUpdateOf)
            ? string.Format(SqlScripts.InsertInTableVariableConsideringUpdateOf, columnsForUpdateOf, ChangeType.Update, insertIntoExceptTableStatement)
            : string.Format(SqlScripts.InsertInTableVariable, ChangeType.Update, insertIntoExceptTableStatement);
    }

    private async Task<Guid> BeginConversationAsync(SqlCommand sqlCommand, CancellationToken ct)
    {
        var result = await PrepareCommand(sqlCommand, SqlScripts.BeginConversation,
            ("@sender", $"{NamingPrefix}_Sender"),
            ("@receiver", $"{NamingPrefix}_Receiver"),
            ("@contract", NamingPrefix)).ExecuteScalarAsync(ct);

        return result is Guid conversationHandler && conversationHandler != Guid.Empty
            ? conversationHandler
            : throw new ServiceBrokerConversationHandlerInvalidException();
    }

    private static SqlCommand PrepareCommand(SqlCommand sqlCommand, string commandText, params ReadOnlySpan<(string Name, object? Value)> parameters)
    {
        sqlCommand.Parameters.Clear();
        sqlCommand.CommandText = commandText;
        foreach (var (name, value) in parameters)
            sqlCommand.Parameters.AddWithValue(name, value ?? DBNull.Value);

        return sqlCommand;
    }

    private static (string Value, bool IsQuoted) NormalizeQueueExecuteAs(string queueExecuteAs)
    {
        var value = queueExecuteAs.Trim();
        var isQuoted = value.Length >= 2 && value[0] is '\'' && value[^1] is '\'';

        return isQuoted
            ? (value[1..^1].Replace("''", "'", StringComparison.Ordinal), true)
            : (value, false);
    }

    private string CreateWhereCondition(bool prependSpace = false)
    {
        var filter = _filter?.Translate();
        if (string.IsNullOrWhiteSpace(filter))
            return string.Empty;

        return $"{(prependSpace ? ' ' : string.Empty)}WHERE {filter}".Trim();
    }

    private string PrepareTriggerLogScript()
    {
        if (!ActivateDatabaseLogging)
            return string.Empty;

        return $"{Environment.NewLine}{Environment.NewLine}DECLARE @LogMessage VARCHAR(255);{Environment.NewLine}"
            + $"SET @LogMessage = 'SqlTableDependency: Message for ' + @dmlType + ' operation added in Queue [{NamingPrefix}].'{Environment.NewLine}"
            + "RAISERROR(@LogMessage, 10, 1) WITH LOG;";
    }

    private string PrepareScriptDropAll(string dropMessages, bool dropFromTrigger)
    {
        string sDropProc = string.Empty;
        string sDropTrig = string.Empty;
        if (dropFromTrigger)
            sDropTrig = string.Format("DISABLE TRIGGER [{1}].[tr_{0}_Sender] ON [{1}].[{2}];DROP TRIGGER [{1}].[tr_{0}_Sender];", NamingPrefix, SchemaName, TableName);
        else
            sDropProc = string.Format("DISABLE TRIGGER [{1}].[tr_{0}_Sender] ON [{1}].[{2}];DROP TRIGGER [{1}].[tr_{0}_Sender];", NamingPrefix, SchemaName, TableName);

        var script = string.Format(SqlScripts.ScriptDropAll, NamingPrefix, dropMessages, DefaultBrokerSchemaName, sDropProc, sDropTrig, SchemaName);
        return ActivateDatabaseLogging
            ? script
            : RemoveLogOperations(script);
    }

    private string PrepareScriptProcedureQueueActivation(string dropAllScript)
    {
        var script = string.Format(SqlScripts.CreateProcedureQueueActivation, NamingPrefix, dropAllScript, DefaultBrokerSchemaName);

        return ActivateDatabaseLogging
            ? script
            : RemoveLogOperations(script);
    }

    internal static string RemoveLogOperations(string source)
    {
        const string logStart = "PRINT N'SqlTableDependency:";
        const string logEnd = ".';";

        var result = new StringBuilder(source.Length);
        var position = 0;
        while (true)
        {
            var startPos = source.IndexOf(logStart, position, StringComparison.InvariantCultureIgnoreCase);
            if (startPos < 1)
                break;

            var endPos = source.IndexOf(logEnd, startPos, StringComparison.InvariantCultureIgnoreCase);
            if (endPos < 1)
                break;

            result.Append(source, position, startPos - position);
            position = endPos + logEnd.Length;
        }

        return result.Append(source, position, source.Length - position).ToString();
    }

    private static string Spacer(int numberOfSpaces)
        => new(' ', numberOfSpaces);

    #endregion

    #region WaitForNotifications

    // aislop-ignore-next-block complexity/function-too-long -- Receive, notification dispatch, reconnect and terminal status handling share one loop.
    private async Task WaitForNotificationsAsync(
        int timeout,
        int watchdogTimeout,
        CancellationToken ct)
    {
        _loopActive = true;

        try
        {
            LogDebug("Get in WaitForNotifications.");

            var messagesBag = CreateMessagesBag(Encoding, _processableMessages);
            var messageNumber = _userInterestedColumns.Count() * (_includeOldEntity ? 2 : 1) + 2;

            // Arm the conversation timer each loop so an idle dialog eventually fires DialogTimer onto the _Sender queue.
            // The activation procedure then ends that conversation; in persisted mode its drop-all script is empty, so the
            // objects survive while the stale initiator dialog is retired (a non-persistent listener instead drops everything).
            // Error 8426 is expected if the activation procedure ends the selected persisted dialog before its timer is armed.
            var receiveStatement = $"WAITFOR (RECEIVE TOP({messageNumber}) [message_type_name], [message_body] FROM [{DefaultBrokerSchemaName}].[{NamingPrefix}_Receiver]), TIMEOUT {timeout * 1000};";
            var waitForSqlScript = _persisted
                ? "DECLARE @conversationHandle UNIQUEIDENTIFIER;"
                  + " SELECT TOP(1) @conversationHandle = conversation_handle"
                  + " FROM sys.conversation_endpoints WITH (NOLOCK)"
                  + " WHERE far_service = @farService AND is_initiator = 1 AND state_desc NOT IN ('CLOSED', 'ERROR')"
                  + " AND service_id = (SELECT service_id FROM sys.services WITH (NOLOCK) WHERE name = @localService);"
                  + " BEGIN TRY IF @conversationHandle IS NOT NULL BEGIN CONVERSATION TIMER (@conversationHandle) TIMEOUT = @watchdogTimeout;"
                  + " END TRY BEGIN CATCH IF ERROR_NUMBER() <> 8426 THROW; END CATCH;"
                : "BEGIN CONVERSATION TIMER (@conversationHandle) TIMEOUT = @watchdogTimeout;";
            waitForSqlScript += receiveStatement;

            var spinGuard = new SpinGuard(_logger, timeout, watchdogTimeout);
            long? outageStartedAt = null;
            var reconnectDelay = TimeSpan.Zero;
            Exception? lastException = null;

            await NotifyListenersAboutStatus(TableDependencyStatus.Started);

            while (true)
            {
                SqlConnection? sqlConnection = null;
                var dispatching = false;

                // Its own token so a heartbeat can release this connection (to enable the broker) without stopping the listener.
                using var receiveCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                var receiveToken = receiveCts.Token;
                _receiveCts = receiveCts;

                try
                {
                    sqlConnection = new SqlConnection(_connectionString);

                    using (StartActivity(nameof(WaitForNotificationsAsync) + "Open database connection", startIndependentTrace: true)
                        ?.SetTag("tabledependency.timeout", timeout)
                        .SetTag("tabledependency.watchdogTimeout", watchdogTimeout))
                    {
                        await sqlConnection.OpenAsync(receiveToken);
                        LogDebug("Connection opened.");
                        await NotifyListenersAboutStatus(TableDependencyStatus.WaitingForNotification);
                    }

                    while (true)
                    {
                        var iterationStartedAt = Stopwatch.GetTimestamp();
                        var receivedDataMessage = false;

                        // Each WAITFOR gets its own trace to not get one trace in otel. The span closes before any spin backoff below, so a
                        // throttled iteration does not report its delay as WAITFOR duration; closing it also closes the reader, so the
                        // connection is not left mid-fetch while we wait.
                        using (StartActivity(nameof(WaitForNotificationsAsync), startIndependentTrace: true)
                            ?.SetTag("tabledependency.timeout", timeout)
                            .SetTag("tabledependency.watchdogTimeout", watchdogTimeout))
                        {
                            await using var sqlCommand = sqlConnection.CreateCommand();
                            sqlCommand.CommandText = waitForSqlScript;
                            sqlCommand.CommandTimeout = 0;
                            sqlCommand.Parameters.AddWithValue("@watchdogTimeout", watchdogTimeout);
                            if (_persisted)
                            {
                                sqlCommand.Parameters.AddWithValue("@farService", $"{NamingPrefix}_Receiver");
                                sqlCommand.Parameters.AddWithValue("@localService", $"{NamingPrefix}_Sender");
                            }
                            else
                                sqlCommand.Parameters.AddWithValue("@conversationHandle", ConversationHandle);
                            LogDebug("Executing WAITFOR command.");

                            await using var sqlDataReader = await sqlCommand.ExecuteReaderAsync(receiveToken);
                            LogDebug("Starting to read.");
                            while (await sqlDataReader.ReadAsync(receiveToken))
                            {
                                var message = new Message(sqlDataReader.GetSqlString(0).Value, await sqlDataReader.IsDBNullAsync(1, receiveToken)
                                    ? null
                                    : sqlDataReader.GetSqlBytes(1).Value);

                                if (message.MessageType.Equals(SqlMessageTypes.ErrorType, StringComparison.OrdinalIgnoreCase))
                                {
                                    if (!_persisted)
                                        throw new QueueContainingErrorMessageException();

                                    LogError(new QueueContainingErrorMessageException(), "Service Broker error message received; keeping listener alive.");
                                    messagesBag.Reset();
                                    continue;
                                }

                                // Ignore service broker messages
                                if (message.MessageType.StartsWith("http://schemas.microsoft.com/SQL/ServiceBroker/", StringComparison.OrdinalIgnoreCase))
                                {
                                    LogDebug("Ignored system Service Broker message type = {MessageType}.", ("MessageType", message.MessageType));
                                    continue;
                                }

                                // Only a data message proves the RECEIVE genuinely returned early. Error and system rows do not: a torn dialog
                                // can deliver them on every iteration, which would reset the spin guard and leave the loop hammering the queue.
                                receivedDataMessage = true;

                                messagesBag.AddMessage(message);
                                LogDebug("Received message type = {MessageType}.", ("MessageType", message.MessageType));

                                if (messagesBag.Status is MessagesBagStatus.Ready)
                                {
                                    LogDebug("Message ready to be notified.");

                                    // A fault raised by a subscriber is never retried as a lost connection, even when it is a SqlException.
                                    dispatching = true;
                                    await NotifyListenersAboutChange(messagesBag);
                                    dispatching = false;

                                    LogDebug("Message notified.");
                                    messagesBag.Reset();
                                }
                            }
                        }

                        // The outage only ends once a receive completes, so a connection that opens but cannot RECEIVE keeps counting.
                        if (outageStartedAt is not null)
                        {
                            LogInformation("Listener recovered after {OutageSeconds}s.", ("OutageSeconds", (long)Stopwatch.GetElapsedTime(outageStartedAt.Value).TotalSeconds));
                            outageStartedAt = null;
                            reconnectDelay = TimeSpan.Zero;
                        }

                        await spinGuard.ThrottleAsync(receivedDataMessage, Stopwatch.GetElapsedTime(iterationStartedAt), receiveToken);
                    }
                }
                catch (Exception exception)
                {
                    using var activity = StartActivity(nameof(WaitForNotificationsAsync), startIndependentTrace: true);

                    // Benign only when the listener's own connection went down; a subscriber/mapper fault leaves it usable and still surfaces.
                    if (ct.IsCancellationRequested)
                    {
                        await NotifyListenersAboutStatus(TableDependencyStatus.StopDueToCancellation);
                        LogInformation("Operation canceled.");
                        return;
                    }

                    if (_shuttingDown && sqlConnection is { State: ConnectionState.Closed or ConnectionState.Broken })
                    {
                        await NotifyListenersAboutStatus(TableDependencyStatus.StopDueToProcessShutdown);
                        LogInformation("Process shutting down; listener stopped.");
                        return;
                    }

                    if (receiveCts.IsCancellationRequested)
                    {
                        LogInformation("Receive connection released for repair of {NamingPrefix}.", (nameof(NamingPrefix), NamingPrefix));
                    }
                    else
                    {
                        lastException = exception;
                        outageStartedAt ??= Stopwatch.GetTimestamp();
                        if (!ReconnectPolicy.ShouldReconnect(exception, dispatching, Stopwatch.GetElapsedTime(outageStartedAt.Value), ReconnectTimeout))
                        {
                            await StopDueToErrorAsync(exception);
                            return;
                        }

                        // Not OnException: that event means the listener stopped, and this one is about to try again.
                        LogWarning(exception, "Receive failed; reconnecting to {NamingPrefix}.", (nameof(NamingPrefix), NamingPrefix));
                    }

                    if (Status is not TableDependencyStatus.Reconnecting)
                        await NotifyListenersAboutStatus(TableDependencyStatus.Reconnecting);

                    messagesBag.Reset();
                }
                finally
                {
                    _receiveCts = null;
                    if (sqlConnection is not null)
                        await sqlConnection.DisposeAsync();
                    _receiveClosed?.TrySetResult();
                }

                // Reopen only after a check that actually ran: one that cannot even connect means the database is not back yet,
                // and reopening then would skip the repair (a disabled broker, say) that the next check would have done.
                while (true)
                {
                    reconnectDelay = ReconnectPolicy.NextDelay(reconnectDelay);
                    try
                    {
                        await Task.Delay(reconnectDelay, ct);
                        var checkError = await RepairBeforeReconnectAsync(ct);
                        if (checkError is null)
                            break;

                        lastException = checkError;
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested)
                    {
                        await NotifyListenersAboutStatus(TableDependencyStatus.StopDueToCancellation);
                        LogInformation("Operation canceled.");
                        return;
                    }

                    if (_shuttingDown)
                    {
                        await NotifyListenersAboutStatus(TableDependencyStatus.StopDueToProcessShutdown);
                        LogInformation("Process shutting down; listener stopped.");
                        return;
                    }

                    if (outageStartedAt is not null && lastException is not null
                        && !ReconnectPolicy.ShouldReconnect(lastException, false, Stopwatch.GetElapsedTime(outageStartedAt.Value), ReconnectTimeout))
                    {
                        await StopDueToErrorAsync(lastException);
                        return;
                    }
                }
            }
        }
        catch (Exception exception) when (ct.IsCancellationRequested)
        {
            // Setup or a status handler failed outside the receive try (it reports its own faults), during cancellation.
            LogDebug("Cancelled during listener setup: {Message}", ("Message", exception.Message));
            await NotifyListenersAboutStatus(TableDependencyStatus.StopDueToCancellation);
        }
        catch (Exception exception)
        {
            // A fault outside the receive try (setup, or a status handler) never reaches its catch; it must still stop the
            // listener the documented way rather than fault the task and escape through StopAsync.
            await StopDueToErrorAsync(exception);
        }
        finally
        {
            _loopActive = false;

            // Under the repair lock, so an in-flight heartbeat repair finishes first and cannot recreate what this drops.
            if (!_persisted)
            {
                await _repairLock.WaitAsync();
                try
                {
                    await DropDatabaseObjectsAsync();
                }
                finally
                {
                    _repairLock.Release();
                }
            }
        }
    }

    // Surface the exception before the terminal status so a subscriber observing StopDueToError already has it.
    private async Task StopDueToErrorAsync(Exception exception)
    {
        await NotifyListenersAboutException(exception);
        await NotifyListenersAboutStatus(TableDependencyStatus.StopDueToError);
        LogError(exception, "Exception in WaitForNotifications.");
    }

    // Returns why the check could not run (the database is not reachable yet), or null once it ran, whatever it found.
    private async Task<Exception?> RepairBeforeReconnectAsync(CancellationToken ct)
    {
        await _repairLock.WaitAsync(ct);
        try
        {
            await CheckAndRepairCoreAsync(ct);
            return null;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            LogDebug("Health check before reconnect failed: {Message}", ("Message", exception.Message));
            return exception;
        }
        finally
        {
            _repairLock.Release();
        }
    }

    // Caller holds _repairLock, so the loop cannot reopen until the caller is done (its reconnect repair waits on the same lock).
    private async Task ReleaseReceiveConnectionAsync(CancellationToken ct)
    {
        var closed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _receiveClosed = closed;
        try
        {
            if (_receiveCts is not { } receiveCts)
                return;

            receiveCts.Cancel();
            await closed.Task.WaitAsync(TimeSpan.FromSeconds(30), ct);
        }
        catch (Exception exception) when (exception is TimeoutException or ObjectDisposedException)
        {
            // The loop closed it between our read and the cancel, or is slow to unwind; the enable below reports any leftover.
        }
        finally
        {
            _receiveClosed = null;
        }
    }

    private MessagesBag CreateMessagesBag(Encoding encoding, ICollection<string> processableMessages)
        => new(encoding,
            [
                string.Format(_startMessageTemplate, NamingPrefix, ChangeType.Insert),
                string.Format(_startMessageTemplate, NamingPrefix, ChangeType.Update),
                string.Format(_startMessageTemplate, NamingPrefix, ChangeType.Delete)
            ],
            string.Format(_endMessageTemplate, NamingPrefix),
            processableMessages);

    public async Task DropDatabaseObjectsAsync()
    {
        using var activity = StartActivity(nameof(DropDatabaseObjectsAsync));

        await using var sqlConnection = new SqlConnection(_connectionString);
        await sqlConnection.OpenAsync();

        await using var sqlTransaction = await sqlConnection.BeginTransactionAsync(IsolationLevel.Serializable);
        await using var sqlCommand = sqlConnection.CreateCommand();

        var dropMessages = string.Join(Environment.NewLine, _processableMessages.Select((pm, index)
            => index > 0
                ? (Spacer(8) + string.Format("IF EXISTS (SELECT * FROM sys.service_message_types WITH (NOLOCK) WHERE name = N'{0}') DROP MESSAGE TYPE [{0}];", pm))
                : string.Format("IF EXISTS (SELECT * FROM sys.service_message_types WITH (NOLOCK) WHERE name = N'{0}') DROP MESSAGE TYPE [{0}];", pm)));

        var dropAllScript = PrepareScriptDropAll(dropMessages, false);

        sqlCommand.Transaction = (SqlTransaction)sqlTransaction;
        sqlCommand.CommandType = CommandType.Text;
        sqlCommand.CommandText = dropAllScript;
        sqlCommand.CommandTimeout = 180; // Plenty of time to drop db objects
        await sqlCommand.ExecuteNonQueryAsync();

        await sqlTransaction.CommitAsync();

        LogInformation("DropDatabaseObjects method executed.");
    }

    #endregion

    #region Logging

    private Activity? StartActivity(string name, bool startIndependentTrace = false)
    {
        var activityName = $"tabledependency.{name.ToLowerInvariant()}";

        Activity? activity;
        if (startIndependentTrace)
        {
            var traceId = ActivityTraceId.CreateRandom();
            var spanId = ActivitySpanId.CreateRandom();
            var parentContext = new ActivityContext(traceId, spanId, ActivityTraceFlags.Recorded);
            activity = ActivitySource.StartActivity(activityName, ActivityKind.Internal, parentContext);
            if (activity is null)
            {
                activity = new Activity(activityName);
                activity.SetParentId(traceId, spanId, ActivityTraceFlags.Recorded);
                activity.Start();
            }
        }
        else
        {
            activity = ActivitySource.StartActivity(activityName);
        }

        return activity?
            .AddTagAndBaggage("db.system", "mssql")
            .AddTagAndBaggage("db.server", _server)
            .AddTagAndBaggage("db.database", _database)
            .AddTagAndBaggage("db.schemaName", SchemaName)
            .AddTagAndBaggage("db.tableName", TableName)
            .AddTagAndBaggage("tabledependency.naming", NamingPrefix)
            .SetTag("tabledependency.includeOldEntity", _includeOldEntity)
            .SetTag("tabledependency.persisted", _persisted);
    }

    private void LogDebug(string message, params (string Name, object? Value)[] values)
        => Telemetry.Log(_logger, LogLevel.Debug, null, message, values);

    private void LogInformation(string message, params (string Name, object? Value)[] values)
        => Telemetry.Log(_logger, LogLevel.Information, null, message, values);

    private void LogWarning(Exception? exception, string message, params (string Name, object? Value)[] values)
        => Telemetry.Log(_logger, LogLevel.Warning, exception, message, values);

    private void LogError(Exception exception, string message, params (string Name, object? Value)[] values)
        => Telemetry.Log(_logger, LogLevel.Error, exception, message, values);

    #endregion

    #region Notifications

    private async Task NotifyListenersAboutException(Exception? exception)
    {
        var eventArgs = new ExceptionEventArgs("TableDependency stopped working", exception, _server, _database, NamingPrefix, CultureInfo);
        OnException?.Invoke(eventArgs);
        if (OnExceptionAsync is not null)
            await OnExceptionAsync.Invoke(eventArgs);
    }

    private async Task NotifyListenersAboutStatus(TableDependencyStatus status)
    {
        Status = status;

        Activity.Current?.AddEvent(new ActivityEvent(nameof(NotifyListenersAboutStatus), tags: new()
        {
            { "tabledependency.status", status.ToString() }
        }));

        var eventArgs = new StatusChangedEventArgs(status, _server, _database, NamingPrefix, CultureInfo);
        OnStatusChanged?.Invoke(eventArgs);
        if (OnStatusChangedAsync is not null)
            await OnStatusChangedAsync.Invoke(eventArgs);
    }

    private async Task NotifyListenersAboutChange(MessagesBag messagesBag)
    {
        using var activity = StartActivity(nameof(OnChanged), startIndependentTrace: true);
        activity?.AddEvent(new ActivityEvent(nameof(NotifyListenersAboutChange), tags: new()
        {
            { "tabledependency.messageType", messagesBag.MessageType.ToString() },
            { "tabledependency.messageCount", messagesBag.Messages.Count }
        }));

        var eventArgs = _isExpando
            ? (RecordChangedEventArgs<T>)(object)new ExpandoRecordChangedEventArgs(messagesBag, _server, _database, NamingPrefix, CultureInfo, _includeOldEntity)
            : new RecordChangedEventArgs<T>(messagesBag, Mapper, _server, _database, NamingPrefix, CultureInfo, _includeOldEntity);

        OnChanged?.Invoke(eventArgs);
        if (OnChangedAsync is not null)
            await OnChangedAsync.Invoke(eventArgs);
    }

    #endregion
}