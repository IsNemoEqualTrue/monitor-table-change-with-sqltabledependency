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

using Microsoft.Extensions.Logging;
using System;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.Interfaces;

namespace TableDependency.SqlClient;

public abstract class SqlTableDependencyBuilder<T>(string connectionString) : ITableDependencyBuilder<T> where T : class, new()
{
    // CreateSqlTableDependencyAsync Properties
    protected string ConnectionString { get; } = connectionString;
    public string? SchemaName { get; protected set; }
    public string? TableName { get; protected set; }
    public IModelToTableMapper<T>? Mapper { get; protected set; }
    public IUpdateOfModel<T>? UpdateOf { get; protected set; }
    public NotifyOn NotifyOn { get; protected set; } = NotifyOn.All;
    public ITableDependencyFilter? Filter { get; protected set; }
    public bool IncludeOldEntity { get; protected set; }
    public string? PersistentId { get; protected set; }

    // Other properties
    public CultureInfo? CultureInfo { get; protected set; }
    public Encoding? Encoding { get; protected set; }
    public bool? ActivateDatabaseLogging { get; protected set; }
    public string? ServiceAuthorization { get; protected set; }
    public string? QueueExecuteAs { get; protected set; }
    public bool? AutoRepair { get; protected set; }
    public TimeSpan? ReconnectTimeout { get; protected set; }

    public async Task<ITableDependency<T>> BuildAsync(ILogger? logger = null, CancellationToken ct = default)
    {
        var tableDependency = await SqlTableDependency<T>.CreateSqlTableDependencyAsync(ConnectionString, SchemaName, TableName, Mapper, UpdateOf, NotifyOn, Filter, logger, IncludeOldEntity, PersistentId, ct);

        // Other properties
        tableDependency.CultureInfo = CultureInfo ?? tableDependency.CultureInfo;
        tableDependency.Encoding = Encoding ?? tableDependency.Encoding;
        tableDependency.ActivateDatabaseLogging = ActivateDatabaseLogging ?? tableDependency.ActivateDatabaseLogging;
        tableDependency.ServiceAuthorization = ServiceAuthorization ?? tableDependency.ServiceAuthorization;
        tableDependency.QueueExecuteAs = QueueExecuteAs ?? tableDependency.QueueExecuteAs;
        tableDependency.AutoRepair = AutoRepair ?? tableDependency.AutoRepair;
        tableDependency.ReconnectTimeout = ReconnectTimeout ?? tableDependency.ReconnectTimeout;

        return tableDependency;
    }
}