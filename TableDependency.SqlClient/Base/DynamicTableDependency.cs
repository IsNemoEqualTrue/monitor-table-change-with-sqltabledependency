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

#endregion License

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using TableDependency.SqlClient.Base.Abstracts;
using TableDependency.SqlClient.Base.Delegates;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;
using TableDependency.SqlClient.Base.Exceptions;
using TableDependency.SqlClient.Base.Messages;
using TableDependency.SqlClient.Base.Utilities;

namespace TableDependency.SqlClient.Base
{
    public abstract class DynamicTableDependency : IDynamicTableDependency
    {
        #region Instance Variables

        /// protected IModelToTableMapper<DataTable> _mapper;
        protected CancellationTokenSource _cancellationTokenSource;

        protected string _connectionString;
        protected string _tableName;
        protected string _schemaName;
        protected string _server;
        protected string _database;
        protected Task _task;
        protected IList<string> _processableMessages;
        protected TableDependencyStatus _status;
        protected DmlTriggerType _dmlTriggerType;
        protected ITableDependencyFilter _filter;
        protected bool _disposed;
        protected string _dataBaseObjectsNamingConvention;
        protected bool _databaseObjectsCreated;

        #endregion Instance Variables

        #region Events

        /// <summary>
        /// Occurs when an error happen during listening for changes on monitored table.
        /// </summary>
        public abstract event ErrorEventHandler OnError;

        /// <summary>
        /// Occurs when the table content has been changed with an update, insert or delete operation.
        /// </summary>
        public abstract event ChangedEventHandler OnChanged;

        /// <summary>
        /// Occurs when SqlTableDependency changes.
        /// </summary>
        public abstract event StatusEventHandler OnStatusChanged;

        #endregion Events

        #region Properties

        /// <summary>
        /// Gets the ModelToTableMapper.
        /// </summary>
        /// public IModelToTableMapper<DataTable> Mapper => _mapper;

        /// <summary>
        /// Gets or sets the trace switch.
        /// </summary>
        /// <value>
        /// The trace switch.
        /// </value>
        public TraceLevel TraceLevel { get; set; } = TraceLevel.Off;

        /// <summary>
        /// Gets or Sets the TraceListener.
        /// </summary>
        /// <value>
        /// The logger.
        /// </value>
        public TraceListener TraceListener { get; set; }

        /// <summary>
        /// Gets or sets the culture info.
        /// </summary>
        /// <value>
        /// The culture information five letters iso code.
        /// </value>
        public CultureInfo CultureInfo { get; set; } = new CultureInfo("en-US");

        /// <summary>
        /// Gets or sets the encoding use to convert database strings.
        /// </summary>
        /// <value>
        /// The encoding.
        /// </value>
        public Encoding Encoding { get; set; }

        /// <summary>
        /// Return the database objects naming convention for created objects used to receive notifications.
        /// </summary>
        /// <value>
        /// The data base objects naming.
        /// </value>
        public string DataBaseObjectsNamingConvention => new string(_dataBaseObjectsNamingConvention.ToCharArray());

        /// <summary>
        /// Gets the SqlTableDependency status.
        /// </summary>
        /// <value>
        /// The TableDependencyStatus enumeration status.
        /// </value>
        public TableDependencyStatus Status => _status;

        /// <summary>
        /// Gets name of the table.
        /// </summary>
        /// <value>
        /// The name of the table.
        /// </value>
        public string TableName => _tableName;

        /// <summary>
        /// Gets or sets the name of the schema.
        /// </summary>
        /// <value>
        /// The name of the schema.
        /// </value>
        public string SchemaName => _schemaName;

        #endregion Properties

        #region Constructors

        protected DynamicTableDependency(
            string connectionString,
            string tableName = null,
            string schemaName = null,
            /// IUpdateOfModel<DataTable> updateOf = null,
            ITableDependencyFilter filter = null,
            DmlTriggerType dmlTriggerType = DmlTriggerType.All,
            bool executeUserPermissionCheck = true)
        {
            // if (updateOf?.Count() == 0) throw new UpdateOfException("updateOf parameter is empty.");

            _connectionString = connectionString;
            this.CheckIfConnectionStringIsValid();
            if (executeUserPermissionCheck) this.CheckIfUserHasPermissions();

            _tableName = this.GetTableName(tableName);
            _schemaName = this.GetSchemaName(schemaName);
            _server = this.GetServerName();
            _database = this.GetDataBaseName();

            this.CheckIfTableExists();
            this.CheckRdbmsDependentImplementation();

            var tableColumnList = this.GetTableColumnsList();
            if (!tableColumnList.Any()) throw new TableWithNoColumnsException(_tableName);

            this.CheckUpdateOfCongruenceWithTriggerType(dmlTriggerType);

            _dataBaseObjectsNamingConvention = this.GetBaseObjectsNamingConvention();
            _dmlTriggerType = dmlTriggerType;
            _filter = filter;
        }

        #endregion Constructors

        #region Public methods

        /// <summary>
        /// Starts monitoring table's content changes.
        /// </summary>
        /// <param name="timeOut">The WAITFOR timeout in seconds.</param>
        /// <param name="watchDogTimeOut">The WATCHDOG timeout in seconds.</param>
        public abstract void Start(int timeOut = 120, int watchDogTimeOut = 180);

        /// <summary>
        /// Stops monitoring table's content changes.
        /// </summary>
        public abstract void Stop();

        #endregion Public methods

        #region Logging

        protected virtual string FormatTraceMessageHeader()
        {
            return $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [Server: {_server} Database: {_database}]";
        }

        protected string DumpException(Exception exception)
        {
            var sb = new StringBuilder();

            sb.AppendLine(Environment.NewLine);
            sb.AppendLine("EXCEPTION:");
            sb.AppendLine(exception.GetType().Name);
            sb.AppendLine(exception.Message);
            sb.AppendLine(exception.StackTrace);

            var innerException = exception.InnerException;
            if (innerException != null) AddInnerException(sb, innerException);

            return sb.ToString();
        }

        protected static void AddInnerException(StringBuilder sb, Exception exception)
        {
            while (true)
            {
                sb.AppendLine(Environment.NewLine);
                sb.AppendLine("INNER EXCEPTION:");
                sb.AppendLine(exception.GetType().Name);
                sb.AppendLine(exception.Message);
                sb.AppendLine(exception.StackTrace);

                var innerException = exception.InnerException;
                if (innerException != null)
                {
                    exception = innerException;
                    continue;
                }

                break;
            }
        }

        protected virtual void WriteTraceMessage(TraceLevel traceLevel, string message, Exception exception = null)
        {
            try
            {
                if (this.TraceListener == null) return;
                if (this.TraceLevel < TraceLevel.Off || this.TraceLevel > TraceLevel.Verbose) return;

                if (this.TraceLevel >= traceLevel)
                {
                    var messageToWrite = new StringBuilder(message);
                    if (exception != null) messageToWrite.Append(this.DumpException(exception));
                    this.TraceListener.WriteLine($"{this.FormatTraceMessageHeader()}{messageToWrite}");
                    this.TraceListener.Flush();
                }
            }
            catch
            {
                // Intentionally ignored
            }
        }

        #endregion Logging

        #region Checks

        protected virtual void CheckUpdateOfCongruenceWithTriggerType(DmlTriggerType dmlTriggerType)
        {
            if (!dmlTriggerType.HasFlag(DmlTriggerType.Update) && !dmlTriggerType.HasFlag(DmlTriggerType.All))
            {
                throw new DmlTriggerTypeException("updateOf parameter can be specified only if DmlTriggerType parameter contains DmlTriggerType.Update too, not for DmlTriggerType.Delete or DmlTriggerType.Insert only.");
            }
        }

        protected abstract void CheckIfUserInterestedColumnsCanBeManaged();

        protected virtual void CheckRdbmsDependentImplementation()
        {
        }

        protected abstract void CheckIfTableExists();

        protected abstract void CheckIfUserHasPermissions();

        protected abstract void CheckIfConnectionStringIsValid();

        #endregion Checks

        #region Get infos

        protected abstract IEnumerable<TableColumnInfo> GetTableColumnsList();

        protected abstract string GetBaseObjectsNamingConvention();

        protected abstract string GetDataBaseName();

        protected abstract string GetServerName();

        protected abstract string GetTableName(string tableName);

        protected virtual string GetTableNameFromDataAnnotation()
        {
            var attribute = typeof(DataTable).GetTypeInfo().GetCustomAttribute(typeof(TableAttribute));
            return ((TableAttribute)attribute)?.Name;
        }

        protected abstract string GetSchemaName(string schemaName);

        protected virtual DynamicRecordChangedEventArgs GetRecordChangedEventArgs(MessagesBag messagesBag)
        {
            return new DynamicRecordChangedEventArgs(
                messagesBag,
                _server,
                _database,
                _dataBaseObjectsNamingConvention,
                this.CultureInfo);
        }

        #endregion Get infos

        #region Notifications

        protected void NotifyListenersAboutStatus(Delegate[] onStatusChangedSubscribedList, TableDependencyStatus status)
        {
            _status = status;

            if (onStatusChangedSubscribedList == null) return;

            foreach (var dlg in onStatusChangedSubscribedList.Where(d => d != null))
            {
                try
                {
                    dlg.GetMethodInfo().Invoke(dlg.Target, new object[] { this, new StatusChangedEventArgs(status, _server, _database, _dataBaseObjectsNamingConvention) });
                }
                catch
                {
                    // Intentionally ignored
                }
            }
        }

        protected void NotifyListenersAboutError(Delegate[] onErrorSubscribedList, Exception exception)
        {
            if (onErrorSubscribedList == null) return;

            foreach (var dlg in onErrorSubscribedList.Where(d => d != null))
            {
                try
                {
                    dlg.GetMethodInfo().Invoke(dlg.Target, new object[] { this, new ErrorEventArgs(exception, _server, _database, _dataBaseObjectsNamingConvention) });
                }
                catch
                {
                    // Intentionally ignored
                }
            }
        }

        protected void NotifyListenersAboutChange(Delegate[] changeSubscribedList, MessagesBag messagesBag)
        {
            if (changeSubscribedList == null) return;

            foreach (var dlg in changeSubscribedList.Where(d => d != null))
            {
                try
                {
                    dlg.GetMethodInfo().Invoke(dlg.Target, new object[] { this, this.GetRecordChangedEventArgs(messagesBag) });
                }
                catch (NoMatchBetweenModelAndTableColumns)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    this.WriteTraceMessage(TraceLevel.Error, $"Received message type = {ex.Message}.");
                    // Intentionally ignored
                }
            }
        }

        #endregion Notifications

        #region Database object generation/disposition

        protected abstract IList<string> CreateDatabaseObjects(int timeOut, int watchDogTimeOut);

        protected abstract void DropDatabaseObjects();

        #endregion Database object generation/disposition

        #region IDisposable implementation

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                this.Stop();

                this.TraceListener?.Dispose();
            }

            _disposed = true;
        }

        ~DynamicTableDependency()
        {
            this.Dispose(false);
        }

        #endregion IDisposable implementation
    }
}