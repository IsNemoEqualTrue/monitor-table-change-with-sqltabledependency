#region License
// TableDependency, SqlTableDependency, OracleTableDependency
// Copyright (c) 2015-2106 Christian Del Bianco. All rights reserved.
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

        protected const string EndMessageTemplate = "{0}/EndDialog";
        protected const string StartMessageTemplate = "{0}/StartDialog";

        protected CancellationTokenSource _cancellationTokenSource;
        protected string _dataBaseObjectsNamingConvention;
        protected bool _automaticDatabaseObjectsTeardown = true;
        protected bool _needsToCreateDatabaseObjects = true;
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

        protected TableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper, IList<string> updateOf, DmlTriggerType dmlTriggerType, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException(nameof(connectionString));
            this.Check451FromRegistry();

            _tableName = this.GetCandidateTableName(tableName);
            _schemaName = this.GetCandidateSchemaName(tableName);

            PreliminaryChecks(connectionString, _tableName);

            this.Initializer(connectionString, tableName, mapper, updateOf, dmlTriggerType, automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects);
        }

        protected TableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper, UpdateOfModel<T> updateOf, DmlTriggerType dmlTriggerType, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException(nameof(connectionString));
            this.Check451FromRegistry();

            _tableName = this.GetCandidateTableName(tableName);
            _schemaName = this.GetCandidateSchemaName(tableName);

            PreliminaryChecks(connectionString, _tableName);

            this.Initializer(connectionString, tableName, mapper, this.GetColumnNameListFromUpdateOfModel(updateOf), dmlTriggerType, automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects);
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
            if (watchDogTimeOut < 60 || watchDogTimeOut < (timeOut + 60)) throw new ArgumentException("watchDogTimeOut must be at least 60 seconds bigger then timeOut");

            if (_task != null)
            {
                Debug.WriteLine("SqlTableDependency: Already called Start() method.");
                return;
            }

            _processableMessages = _needsToCreateDatabaseObjects
                ? this.CreateDatabaseObjects(_connectionString, _tableName, _dataBaseObjectsNamingConvention, _userInterestedColumns, _updateOf, timeOut, watchDogTimeOut)
                : this.RetrieveProcessableMessages(_userInterestedColumns, _dataBaseObjectsNamingConvention);
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

            if (_automaticDatabaseObjectsTeardown) DropDatabaseObjects(_connectionString, _dataBaseObjectsNamingConvention);

            _disposed = true;

            Debug.WriteLine("OracleTableDependency: Stopped waiting for notification.");
        }

        #endregion

        #region Protected methods

        protected void NotifyListenersAboutStatus(Delegate[] onStatusChangedSubscribedList, TableDependencyStatus status)
        {
            _status = status;

            if (onStatusChangedSubscribedList == null) return;

            foreach (var dlg in onStatusChangedSubscribedList.Where(d => d != null))
            {
                dlg.Method.Invoke(dlg.Target, new object[] { null, new StatusChangedEventArgs(status) });
            }
        }

        protected void NotifyListenersAboutError(Delegate[] onErrorSubscribedList, Exception exception)
        {
            if (onErrorSubscribedList != null)
            {
                foreach (var dlg in onErrorSubscribedList.Where(d => d != null))
                {
                    dlg.Method.Invoke(dlg.Target, new object[] { null, new ErrorEventArgs(exception) });
                }
            }
        }

        protected abstract IList<string> RetrieveProcessableMessages(IEnumerable<ColumnInfo> userInterestedColumns, string databaseObjectsNaming);

        protected abstract IList<string> CreateDatabaseObjects(string connectionString, string tableName, string databaseObjectsNaming, IEnumerable<ColumnInfo> userInterestedColumns, IList<string> updateOf, int timeOut, int watchDogTimeOut);

        protected virtual void Initializer(string connectionString, string tableName, ModelToTableMapper<T> mapper, IList<string> updateOf, DmlTriggerType dmlTriggerType, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects)
        {
            if (mapper != null && mapper.Count() == 0) throw new ModelToTableMapperException("Empty mapper");

            if (!dmlTriggerType.HasFlag(DmlTriggerType.Update) && !dmlTriggerType.HasFlag(DmlTriggerType.All))
            {
                if (updateOf != null && updateOf.Any())
                {
                    throw new DmlTriggerTypeException("updateOf parameter can be specified only if DmlTriggerType parameter contains DmlTriggerType.Update too, not for DmlTriggerType.Delete or DmlTriggerType.Insert only.");
                }
            }

            _connectionString = connectionString;
            _mapper = mapper ?? this.GetModelMapperFromColumnDataAnnotation();
            _updateOf = updateOf;
            _userInterestedColumns = GetUserInterestedColumns(updateOf);
            _automaticDatabaseObjectsTeardown = automaticDatabaseObjectsTeardown;
            _dataBaseObjectsNamingConvention = GeneratedataBaseObjectsNamingConvention(namingConventionForDatabaseObjects);
            _needsToCreateDatabaseObjects = CheckIfNeedsToCreateDatabaseObjects();
            _dmlTriggerType = dmlTriggerType;
        }

        protected abstract IEnumerable<ColumnInfo> GetUserInterestedColumns(IEnumerable<string> updateOf);

        protected abstract string GeneratedataBaseObjectsNamingConvention(string namingConventionForDatabaseObjects);

        protected abstract bool CheckIfNeedsToCreateDatabaseObjects();

        protected abstract void PreliminaryChecks(string connectionString, string candidateTableName);

        protected abstract void DropDatabaseObjects(string connectionString, string dataBaseObjectsNamingConvention);

        protected virtual string GetCandidateTableName(string tableName)
        {
            return !string.IsNullOrWhiteSpace(tableName) ? tableName : (!string.IsNullOrWhiteSpace(GetTableNameFromTableDataAnnotation()) ? GetTableNameFromTableDataAnnotation() : typeof(T).Name);
        }

        protected virtual string GetCandidateSchemaName(string tableName)
        {
            return (!string.IsNullOrWhiteSpace(GetSchemaNameFromTableDataAnnotation()) ? GetTableNameFromTableDataAnnotation() : string.Empty);
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

        /// <summary>
        /// Check .NET Framework versions by querying the registry in code (.NET Framework 4.5 and later)
        /// </summary>
        /// <remarks>
        /// The existence of the Release DWORD indicates that the.NET Framework 4.5 or later has been installed on a computer.
        /// The value of the keyword indicates the installed version. 
        /// To check this keyword, use the OpenBaseKey and OpenSubKey methods of the Microsoft.Win32.RegistryKey class to access 
        /// the Software\Microsoft\NET Framework Setup\NDP\v4\Full subkey under HKEY_LOCAL_MACHINE in the Windows registry.
        /// </remarks>
        /// <seealso cref="http://msdn.microsoft.com/en-us/library/hh925568(v=vs.110).aspx"/>
        protected virtual void Check451FromRegistry()
        {
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

        #endregion

        #region IDisposable implementation

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;

            if (disposing)
            {
                Stop();
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