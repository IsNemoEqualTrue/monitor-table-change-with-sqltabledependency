#region License
// TableDependency, SqlTableDependency, OracleTableDependency
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
using Microsoft.Win32;
using TableDependency.Classes;
using TableDependency.Delegates;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Exceptions;
using TableDependency.Mappers;

namespace TableDependency
{
    public abstract class TableDependency<T> : ITableDependency<T>, IDisposable where T : class
    {
        #region Protected variables

        protected CancellationTokenSource _cancellationTokenSource;
        protected string _dataBaseObjectsNamingConvention;
        protected ModelToTableMapper<T> _mapper;
        protected string _connectionString;
        protected string _tableName;
        protected string _schemaName;
        protected Task _task;
        protected IList<string> _processableMessages;
        protected IEnumerable<ColumnInfo> _userInterestedColumns;
        protected IList<string> _updateOf;
        protected TableDependencyStatus _status;
        protected DmlTriggerType _dmlTriggerType;
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
        /// Gets or sets the trace switch.
        /// </summary>
        /// <value>
        /// The trace switch.
        /// </value>
        public TraceLevel TraceLevel { get; set; }

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
        public string DataBaseObjectsNamingConvention => string.Copy(_dataBaseObjectsNamingConvention);

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

        protected TableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper, IList<string> updateOf, DmlTriggerType dmlTriggerType)
        {
            this.TableDependencyCommonSettings(connectionString, tableName);
            this.Initializer(connectionString, tableName, mapper, updateOf, dmlTriggerType);
        }

        protected TableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper, UpdateOfModel<T> updateOf, DmlTriggerType dmlTriggerType)
        {
            this.TableDependencyCommonSettings(connectionString, tableName);
            this.Initializer(connectionString, tableName, mapper, this.GetColumnNameListFromUpdateOfModel(updateOf), dmlTriggerType);
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

            DropDatabaseObjects(_connectionString, _dataBaseObjectsNamingConvention);

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

        protected void NotifyListenersAboutStatus(Delegate[] onStatusChangedSubscribedList, TableDependencyStatus status)
        {
            _status = status;

            if (onStatusChangedSubscribedList == null) return;

            foreach (var dlg in onStatusChangedSubscribedList.Where(d => d != null))
            {
                try
                {
                    dlg.Method.Invoke(dlg.Target, new object[] { null, new StatusChangedEventArgs(status) });
                }
                catch
                {
                    // ignored
                }
            }
        }

        protected void NotifyListenersAboutError(Delegate[] onErrorSubscribedList, Exception exception)
        {
            if (onErrorSubscribedList != null)
            {
                foreach (var dlg in onErrorSubscribedList.Where(d => d != null))
                {
                    try
                    {
                        dlg.Method.Invoke(dlg.Target, new object[] { null, new ErrorEventArgs(exception) });
                    }
                    catch
                    {
                        // ignored
                    }
                }
            }
        }

        protected abstract IList<string> RetrieveProcessableMessages(IEnumerable<ColumnInfo> userInterestedColumns, string databaseObjectsNaming);

        protected abstract IList<string> CreateDatabaseObjects(string connectionString, string tableName, string databaseObjectsNaming, IEnumerable<ColumnInfo> userInterestedColumns, IList<string> updateOf, int timeOut, int watchDogTimeOut);

        protected virtual void Initializer(string connectionString, string tableName, ModelToTableMapper<T> mapper, IList<string> updateOf, DmlTriggerType dmlTriggerType)
        {
            if (mapper != null && mapper.Count() == 0) throw new ModelToTableMapperException("Empty mapper");

            if (!dmlTriggerType.HasFlag(DmlTriggerType.Update) && !dmlTriggerType.HasFlag(DmlTriggerType.All))
            {
                if (updateOf != null && updateOf.Any())
                {
                    throw new DmlTriggerTypeException("updateOf parameter can be specified only if DmlTriggerType parameter contains DmlTriggerType.Update too, not for DmlTriggerType.Delete or DmlTriggerType.Insert only.");
                }
            }

            this.TraceLevel = TraceLevel.Off;

            _connectionString = connectionString;
            _mapper = mapper ?? this.GetModelMapperFromColumnDataAnnotation();
            _updateOf = updateOf;
            _userInterestedColumns = GetUserInterestedColumns(updateOf);
            _dataBaseObjectsNamingConvention = GeneratedataBaseObjectsNamingConvention();
            _dmlTriggerType = dmlTriggerType;
        }

        protected abstract IEnumerable<ColumnInfo> GetUserInterestedColumns(IEnumerable<string> updateOf);

        protected abstract string GeneratedataBaseObjectsNamingConvention();

        protected abstract void PreliminaryChecks(string connectionString, string candidateTableName);

        protected abstract void DropDatabaseObjects(string connectionString, string dataBaseObjectsNamingConvention);

        protected virtual string GetCandidateTableName(string tableName)
        {
            return !string.IsNullOrWhiteSpace(tableName) ? tableName : (!string.IsNullOrWhiteSpace(GetTableNameFromTableDataAnnotation()) ? GetTableNameFromTableDataAnnotation() : typeof(T).Name);
        }

        protected virtual string GetCandidateSchemaName(string tableName)
        {
            return !string.IsNullOrWhiteSpace(GetSchemaNameFromTableDataAnnotation()) ? GetTableNameFromTableDataAnnotation() : string.Empty;
        }

        protected virtual string GetTableNameFromTableDataAnnotation()
        {
            var attribute = typeof(T).GetCustomAttribute(typeof(TableAttribute));
            return ((TableAttribute)attribute)?.Name;
        }

        protected virtual string GetSchemaNameFromTableDataAnnotation()
        {
            var attribute = typeof(T).GetCustomAttribute(typeof(TableAttribute));
            return ((TableAttribute)attribute)?.Schema;
        }

        protected virtual ModelToTableMapper<T> GetModelMapperFromColumnDataAnnotation()
        {
            var propertyInfos = typeof(T)
                .GetProperties(BindingFlags.GetProperty | BindingFlags.Instance | BindingFlags.Public)
                .Where(x => Attribute.IsDefined(x, typeof(ColumnAttribute), false))
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

        protected virtual IList<string> GetColumnNameListFromUpdateOfModel(UpdateOfModel<T> updateOf)
        {
            var updateOfList = new List<string>();

            if (updateOf != null && updateOf.Count() > 0)
            {
                foreach (var propertyInfo in updateOf.GetPropertiesInfos())
                {
                    var attribute = propertyInfo.GetCustomAttribute(typeof(ColumnAttribute));
                    if (attribute != null)
                    {
                        var dbColumnName = ((ColumnAttribute)attribute).Name;
                        updateOfList.Add(dbColumnName);
                    }
                    else
                    {
                        updateOfList.Add(propertyInfo.Name);
                    }
                }
            }

            return updateOfList;
        }

        protected virtual void Check451FromRegistry()
        {
            // http://msdn.microsoft.com/en-us/library/hh925568(v=vs.110).aspx
            using (var ndpKey = RegistryKey.OpenBaseKey(RegistryHive.LocalMachine, RegistryView.Registry32).OpenSubKey(@"SOFTWARE\Microsoft\NET Framework Setup\NDP\v4\Full\"))
            {
                if (ndpKey?.GetValue("Release") != null)
                {
                    var releaseKey = (int)ndpKey.GetValue("Release");
                    if ((releaseKey >= 378675)) return;
                }
            }

            throw new Net451Exception();
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
                    this.TraceListener.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff") + ":" + messageToWrite);
                    this.TraceListener.Flush();
                }
            }
            catch
            {
                // ignored
            }
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

        protected void TableDependencyCommonSettings(string connectionString, string tableName)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException(nameof(connectionString));
            this.Check451FromRegistry();

            _tableName = this.GetCandidateTableName(tableName);
            _schemaName = this.GetCandidateSchemaName(tableName);

            this.PreliminaryChecks(connectionString, _tableName);
        }

        #endregion

        #region IDisposable implementation

        public void Dispose()
        {
            Dispose(true);
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
                Stop();

                if (this.TraceListener != null)
                {
                    this.TraceListener.Close();
                    this.TraceListener.Dispose();
                }
            }

            _disposed = true;
        }

        ~TableDependency()
        {
            Dispose(false);
        }

        #endregion
    }
}