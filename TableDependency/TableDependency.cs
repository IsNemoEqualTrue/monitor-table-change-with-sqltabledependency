#region License
// TableDependency, SqlTableDependency
// Copyright (c) 2015-2017 Christian Del Bianco. All rights reserved.
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
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TableDependency.Abstracts;
using TableDependency.Delegates;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Exceptions;
using TableDependency.Utilities;

namespace TableDependency
{
    public abstract class TableDependency<T> : ITableDependency<T>, IDisposable where T : class
    {
        #region Instance Variables

        protected IModelToTableMapper<T> _mapper;
        protected CancellationTokenSource _cancellationTokenSource;
        protected string _dataBaseObjectsNamingConvention;
        protected string _connectionString;
        protected string _tableName;
        protected string _schemaName;
        protected string _server;
        protected string _database;
        protected Task _task;
        protected IList<string> _processableMessages;
        protected IEnumerable<ColumnInfo> _userInterestedColumns;
        protected IList<string> _updateOf;
        protected TableDependencyStatus _status;
        protected DmlTriggerType _dmlTriggerType;
        protected ITableDependencyFilter _filter;
        protected bool _disposed;

        #endregion

        #region Events

        /// <summary>
        /// Occurs when an error happen during listening for changes on monitored table.
        /// </summary>
        public abstract event ErrorEventHandler OnError;

        /// <summary>
        /// Occurs when the table content has been changed with an update, insert or delete operation.
        /// </summary>
        public abstract event ChangedEventHandler<T> OnChanged;

        /// <summary>
        /// Occurs when SqlTableDependency changes.
        /// </summary>
        public abstract event StatusEventHandler OnStatusChanged;

        #endregion

        #region Properties

        /// <summary>
        /// Gets the ModelToTableMapper.
        /// </summary>
        public IModelToTableMapper<T> Mapper => _mapper;

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
        /// Gets or sets the encoding use to convert database strings.
        /// </summary>
        /// <value>
        /// The encoding.
        /// </value>
        public abstract Encoding Encoding { get; set; }

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

        #endregion

        #region Constructors

        protected TableDependency(
            string connectionString,
            string tableName = null,
            IModelToTableMapper<T> mapper = null,
            IUpdateOfModel<T> updateOf = null,
            ITableDependencyFilter filter = null,
            DmlTriggerType dmlTriggerType = DmlTriggerType.All,
            bool teardown = true,
            string objectNaming = null)
        {
            this.CheckIfConnectionStringIsValid(connectionString);
            this.CheckIfParameterlessConstructorExistsForModel();
            this.CheckIfUserHasPermissions(connectionString);

            _connectionString = connectionString;
            _tableName = this.GetCandidateTableName(tableName);
            _schemaName = this.GetCandidateSchemaName(tableName);
            _server = this.GetServerName(connectionString);
            _database = this.GetDataBaseName(connectionString);

            this.CheckIfTableExists(connectionString);
            this.CheckRdbmsDependentImplementation(connectionString);

            var tableColumnList = this.GetTableColumnsList(connectionString);
            if (!tableColumnList.Any()) throw new TableWithNoColumnsException(tableName);

            _mapper = mapper ?? this.GetModelMapperFromColumnDataAnnotation();
            this.CheckMapperValidity(tableColumnList);

            this.CheckUpdateOfCongruenceWithTriggerType(updateOf, dmlTriggerType);
            _updateOf = this.GetUpdateOfColumnNameList(updateOf, tableColumnList);

            _userInterestedColumns = this.GetUserInterestedColumns(tableColumnList);
            this.CheckIfUserInterestedColumnsCanBeManaged(_userInterestedColumns);

            _dataBaseObjectsNamingConvention = this.GeneratedataBaseObjectsNamingConvention();
            _dmlTriggerType = dmlTriggerType;
            _filter = filter;
        }

        #endregion

        #region Public methods

        /// <summary>
        /// Starts monitoring table's content changes.
        /// </summary>
        /// <param name="timeOut">The WAITFOR timeout in seconds.</param>
        /// <param name="watchDogTimeOut">The WATCHDOG timeout in seconds.</param>
        /// <returns></returns>
        /// <exception cref="NoSubscriberException"></exception>
        /// <exception cref="TableDependency.Exceptions.NoSubscriberException"></exception>
        public virtual void Start(int timeOut = 120, int watchDogTimeOut = 180)
        {
            if (timeOut < 60) throw new ArgumentException("timeOut must be greater or equal to 60 seconds");
            if (watchDogTimeOut < 60 || watchDogTimeOut < (timeOut + 60)) throw new WatchDogTimeOutException("watchDogTimeOut must be at least 60 seconds bigger then timeOut");

            if (_task != null)
            {
                Trace.TraceInformation("Already called Start() method.");
                return;
            }

            _processableMessages = this.CreateDatabaseObjects(_connectionString, _tableName, _dataBaseObjectsNamingConvention, _userInterestedColumns, _updateOf, timeOut, watchDogTimeOut);
        }

        /// <summary>
        /// Stops monitoring table's content changes.
        /// </summary>
        public virtual void Stop()
        {
            if (_task != null)
            {
                _cancellationTokenSource.Cancel(true);
                _task?.Wait();
            }

            _task = null;

            this.DropDatabaseObjects(_connectionString, _dataBaseObjectsNamingConvention);

            _disposed = true;

            this.WriteTraceMessage(TraceLevel.Info, "Stopped waiting for notification.");
        }

#if DEBUG
        public virtual void StopWithoutDisposing()
        {
            if (_task != null)
            {
                _cancellationTokenSource.Cancel(true);
                _task?.Wait();
            }

            _task = null;

            _disposed = true;

            this.WriteTraceMessage(TraceLevel.Info, "Stopped waiting for notification.");
        }
#endif

        #endregion

        #region Protected methods

        protected abstract void CheckMapperValidity(IEnumerable<ColumnInfo> tableColumnsList);

        protected abstract void CheckUpdateOfCongruenceWithTriggerType(IUpdateOfModel<T> updateOf, DmlTriggerType dmlTriggerType);

        protected abstract void CheckIfUserInterestedColumnsCanBeManaged(IEnumerable<ColumnInfo> tableColumnsToUse);

        protected abstract IEnumerable<ColumnInfo> GetUserInterestedColumns(IEnumerable<ColumnInfo> tableColumnsLis);

        protected virtual void CheckRdbmsDependentImplementation(string connectionString)
        {
            
        }

        protected abstract void CheckIfTableExists(string connectionString);

        protected abstract void CheckIfUserHasPermissions(string connectionString);

        protected virtual void CheckIfParameterlessConstructorExistsForModel()
        {
            if (typeof(T).GetConstructor(Type.EmptyTypes) == null)
            {
                throw new ModelWithoutParameterlessConstructor("Your model needs a parameterless constructor.");
            }
        }

        protected abstract void CheckIfConnectionStringIsValid(string connectionString);

        protected abstract string GetDataBaseName(string connectionString);

        protected abstract string GetServerName(string connectionString);

        protected void NotifyListenersAboutStatus(Delegate[] onStatusChangedSubscribedList, TableDependencyStatus status)
        {
            _status = status;

            if (onStatusChangedSubscribedList == null) return;

            foreach (var dlg in onStatusChangedSubscribedList.Where(d => d != null))
            {
                try
                {
                    dlg.GetMethodInfo().Invoke(dlg.Target, new object[] { null, new StatusChangedEventArgs(status, _server, _database, _dataBaseObjectsNamingConvention) });
                }
                catch
                {
                    // ignored
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
                    dlg.GetMethodInfo().Invoke(dlg.Target, new object[] { null, new ErrorEventArgs(exception, _server, _database, _dataBaseObjectsNamingConvention) });
                }
                catch
                {
                    // ignored
                }
            }
        }

        protected abstract IList<string> RetrieveProcessableMessages(IEnumerable<ColumnInfo> userInterestedColumns, string databaseObjectsNaming);

        protected abstract IList<string> CreateDatabaseObjects(string connectionString, string tableName, string databaseObjectsNaming, IEnumerable<ColumnInfo> userInterestedColumns, IList<string> updateOf, int timeOut, int watchDogTimeOut);

        protected abstract string GeneratedataBaseObjectsNamingConvention();

        protected abstract void DropDatabaseObjects(string connectionString, string dataBaseObjectsNamingConvention);

        protected abstract string GetCandidateTableName(string tableName);

        protected abstract string GetCandidateSchemaName(string tableName);

        protected virtual string GetTableNameFromTableDataAnnotation()
        {
            var attribute = typeof(T).GetTypeInfo().GetCustomAttribute(typeof(TableAttribute));
            return ((TableAttribute)attribute)?.Name;
        }

        protected virtual string GetSchemaNameFromTableDataAnnotation()
        {
            var attribute = typeof(T).GetTypeInfo().GetCustomAttribute(typeof(TableAttribute));
            return ((TableAttribute)attribute)?.Schema;
        }

        protected virtual IModelToTableMapper<T> GetModelMapperFromColumnDataAnnotation()
        {
            var propertyInfos = typeof(T)
                .GetProperties(BindingFlags.GetProperty | BindingFlags.Instance | BindingFlags.Public)
                .Where(x => CustomAttributeExtensions.IsDefined(x, typeof(ColumnAttribute), false))
                .ToArray();

            if (!propertyInfos.Any()) return null;

            var mapper = new ModelToTableMapper<T>();
            foreach (var propertyInfo in propertyInfos)
            {
                var attribute = propertyInfo.GetCustomAttribute(typeof(ColumnAttribute));
                var dbColumnName = ((ColumnAttribute)attribute).Name;
                if (attribute != null) mapper.AddMapping(propertyInfo, dbColumnName);
            }

            return mapper;
        }

        protected void WriteTraceMessage(TraceLevel traceLevel, string message, Exception exception = null)
        {
            try
            {
                if (this.TraceListener == null) return;
                if (this.TraceLevel < TraceLevel.Off || this.TraceLevel > TraceLevel.Verbose) return;

                if (this.TraceLevel >= traceLevel)
                {
                    var messageToWrite = new StringBuilder(message);
                    if (exception != null) messageToWrite.Append(this.DumpException(exception));
                    this.TraceListener.WriteLine($"{this.MessageHeader()}{messageToWrite}");
                    this.TraceListener.Flush();
                }
            }
            catch
            {
                // ignored
            }
        }

        protected virtual string MessageHeader()
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

        protected abstract IEnumerable<ColumnInfo> GetTableColumnsList(string connectionString);

        protected virtual string GetTableColumnName(IEnumerable<ColumnInfo> tableColumnsList, string modelPropertyName)
        {
            var entityPropertyInfo = ModelUtil.GetModelPropertiesInfo<T>().First(mpf => mpf.Name == modelPropertyName);

            var propertyMappedTo = _mapper?.GetMapping(entityPropertyInfo);
            var propertyName = propertyMappedTo ?? entityPropertyInfo.Name;

            // If model property is mapped to table column keep it
            foreach (var tableColumn in tableColumnsList)
            {
                if (string.Equals(tableColumn.Name.ToLowerInvariant(), propertyName.ToLowerInvariant(), StringComparison.OrdinalIgnoreCase))
                {
                    return tableColumn.Name;
                }
            }

            return modelPropertyName;
        }

        protected virtual IList<string> GetUpdateOfColumnNameList(IUpdateOfModel<T> updateOf, IEnumerable<ColumnInfo> tableColumns)
        {
            var updateOfList = new List<string>();

            if (updateOf == null || updateOf.Count() <= 0) return updateOfList;

            foreach (var propertyInfo in updateOf.GetPropertiesInfos())
            {
                var existingMap = _mapper.GetMapping(propertyInfo);
                if (existingMap != null)
                {
                    updateOfList.Add(existingMap);
                    continue;
                }

                var attribute = propertyInfo.GetCustomAttribute(typeof(ColumnAttribute));
                if (attribute != null)
                {
                    var dbColumnName = ((ColumnAttribute)attribute).Name;
                    updateOfList.Add(dbColumnName);
                }
                else
                {
                    var dbColumnName = GetTableColumnName(tableColumns, propertyInfo.Name);
                    updateOfList.Add(dbColumnName);
                }
            }

            return updateOfList;
        }

        #endregion

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

                TraceListener?.Dispose();
            }

            _disposed = true;
        }

        ~TableDependency()
        {
            this.Dispose(false);
        }

        #endregion
    }
}