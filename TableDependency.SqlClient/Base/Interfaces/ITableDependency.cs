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

using System;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;
using TableDependency.SqlClient.Base.Health;

namespace TableDependency.SqlClient.Base.Interfaces;

// aislop-ignore-next-block complexity/function-too-long -- This is the intentionally complete public listener contract.
public interface ITableDependency<T> : IAsyncDisposable where T : class, new()
{
    #region Events

    /// <summary>
    /// Occurs when an error happen during listening for changes on monitored table.
    /// </summary>
    public event Action<ExceptionEventArgs>? OnException;

    /// <summary>
    /// Occurs when an error happen during listening for changes on monitored table.
    /// </summary>
    public Func<ExceptionEventArgs, Task>? OnExceptionAsync { get; set; }

    /// <summary>
    /// Occurs when SqlTableDependency's status changes.
    /// </summary>
    public event Action<StatusChangedEventArgs>? OnStatusChanged;

    /// <summary>
    /// Occurs when SqlTableDependency's status changes.
    /// </summary>
    public Func<StatusChangedEventArgs, Task>? OnStatusChangedAsync { get; set; }

    /// <summary>
    /// Occurs when the table content has been changed with an update, insert or delete operation.
    /// </summary>
    public event Action<RecordChangedEventArgs<T>>? OnChanged;

    /// <summary>
    /// Occurs when the table content has been changed with an update, insert or delete operation.
    /// </summary>
    public Func<RecordChangedEventArgs<T>, Task>? OnChangedAsync { get; set; }

    #endregion Events

    #region Properties

    /// <summary>
    /// Returns true if SIGTERM has been called.
    /// </summary>
    public bool ShuttingDown { get; }

    /// <summary>
    /// Gets or sets the culture info. The default is the current cultuer.
    /// </summary>
    public CultureInfo CultureInfo { get; set; }

    /// <summary>
    /// Gets or sets the encoding use to convert database strings.
    /// </summary>
    public Encoding Encoding { get; set; }

    /// <summary>
    /// Gets the database objects naming convention for created objects used to receive notifications.
    /// </summary>
    public string NamingPrefix { get; }

    /// <summary>
    /// Gets the SqlTableDependency's status.
    /// </summary>
    public TableDependencyStatus Status { get; }

    /// <summary>
    /// Gets the name of the schema.
    /// </summary>
    public string SchemaName { get; }

    /// <summary>
    /// Gets the name of the table.
    /// </summary>
    public string TableName { get; }

    /// <summary>
    /// The most recent <see cref="CheckDatabaseObjectsAsync"/> result, or null before the first call.
    /// </summary>
    public DatabaseObjectsHealth? LastObjectsHealth => null;

    #endregion Properties

    #region Methods

    /// <summary>
    /// Starts monitoring table's content changes.
    /// </summary>
    /// <param name="timeout">The WAITFOR timeout in seconds.</param>
    /// <param name="watchdogTimeout">The WATCHDOG timeout in seconds.</param>
    /// <param name="waitForStop">Wait until cancellation or error.</param>
    /// <param name="ct">Cancellation token</param>
    public Task StartAsync(int timeout = 120, int watchdogTimeout = 180, bool waitForStop = false, CancellationToken ct = default);

    /// <summary>
    /// Stops monitoring table's content changes.
    /// </summary>
    public Task StopAsync();

    /// <summary>
    /// Forcibly drop database objects on dispose
    /// Only use if you want to remove persistent objects that have been created
    /// </summary>
    public Task DropDatabaseObjectsAsync();

    /// <summary>
    /// Checks the database objects the listener depends on and, when repair is enabled and the listener is running, rebuilds any that are missing or disabled.
    /// Call it from the host's heartbeat; the library does not poll on its own.
    /// </summary>
    public Task<DatabaseObjectsHealth> CheckDatabaseObjectsAsync(CancellationToken ct = default)
        => throw new NotSupportedException($"{GetType().Name} does not support database object health checks.");

    #endregion Methods
}