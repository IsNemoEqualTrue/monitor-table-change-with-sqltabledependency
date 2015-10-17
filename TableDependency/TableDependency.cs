////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco. All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using TableDependency.Delegates;
using TableDependency.Enums;
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
        protected Task _task;
        protected IEnumerable<Tuple<string, string, string>> _userInterestedColumns;
        protected IEnumerable<string> _updateOf;
        protected TableDependencyStatus _status;
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

        #endregion

        #region Properties

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
        public TableDependencyStatus Status => this._status;

        /// <summary>
        /// Gets name of the table.
        /// </summary>
        /// <value>
        /// The name of the table.
        /// </value>
        public string TableName => this._tableName;

        #endregion

        #region Public methods

        public abstract void Start(int timeOut = 120, int watchDogTimeOut = 180);

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

        #region Constructors

        protected TableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper, IEnumerable<string> updateOf, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException(nameof(connectionString));
            _tableName = this.GetCandidateTableName(tableName);
            PreliminaryChecks(connectionString, _tableName);
            this.Initializer(connectionString, tableName, mapper, updateOf, automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects);
        }

        protected TableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper, UpdateOfModel<T> updateOf, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException(nameof(connectionString));
            _tableName = this.GetCandidateTableName(tableName);
            PreliminaryChecks(connectionString, _tableName);
            this.Initializer(connectionString, tableName, mapper, this.GetColumnNameListFromUpdateOfModel(updateOf), automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects);
        }

        #endregion

        #region Protected methods

        protected virtual void Initializer(string connectionString, string tableName, ModelToTableMapper<T> mapper, IEnumerable<string> updateOf, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects)
        {
            if (mapper != null && mapper.Count() == 0) throw new ModelToTableMapperException("Empty mapper");

            _connectionString = connectionString;
            _mapper = mapper ?? this.GetModelMapperFromColumnDataAnnotation();
            _updateOf = updateOf;
            _userInterestedColumns = GetColumnsToUseForCreatingDbObjects(updateOf);
            _automaticDatabaseObjectsTeardown = automaticDatabaseObjectsTeardown;
            _dataBaseObjectsNamingConvention = GeneratedataBaseObjectsNamingConvention(namingConventionForDatabaseObjects);
            _needsToCreateDatabaseObjects = CheckIfNeedsToCreateDatabaseObjects();
            _status = TableDependencyStatus.WaitingForStart;
        }

        protected abstract IEnumerable<Tuple<string, string, string>> GetColumnsToUseForCreatingDbObjects(IEnumerable<string> updateOf);

        protected abstract string GeneratedataBaseObjectsNamingConvention(string namingConventionForDatabaseObjects);

        protected abstract bool CheckIfNeedsToCreateDatabaseObjects();

        protected abstract void PreliminaryChecks(string connectionString, string candidateTableName);

        protected abstract void DropDatabaseObjects(string connectionString, string dataBaseObjectsNamingConvention);

        protected string GetCandidateTableName(string tableName)
        {
            return !string.IsNullOrWhiteSpace(tableName) ? tableName : (!string.IsNullOrWhiteSpace(GetTableNameFromTableDataAnnotation()) ? GetTableNameFromTableDataAnnotation() : typeof(T).Name);
        }

        protected string GetTableNameFromTableDataAnnotation()
        {
            var attribute = typeof(T).GetCustomAttribute(typeof(TableAttribute));
            return ((TableAttribute)attribute)?.Name.ToUpper();
        }

        protected ModelToTableMapper<T> GetModelMapperFromColumnDataAnnotation()
        {
            ModelToTableMapper<T> mapper = null;

            var propertyInfos = typeof(T)
                .GetProperties(BindingFlags.GetProperty | BindingFlags.Instance | BindingFlags.Public)
                .Where(x => Attribute.IsDefined(x, typeof(ColumnAttribute), false))
                .ToArray();

            if (!propertyInfos.Any()) return null;

            mapper = new ModelToTableMapper<T>();
            foreach (var propertyInfo in propertyInfos)
            {
                var attribute = propertyInfo.GetCustomAttribute(typeof(ColumnAttribute));
                var dbColumnName = ((ColumnAttribute)attribute).Name;
                if (attribute != null) mapper.AddMapping(propertyInfo, dbColumnName);
            }

            return mapper;
        }

        protected IList<string> GetColumnNameListFromUpdateOfModel(UpdateOfModel<T> updateOf)
        {
            var updateOfList = new List<string>();

            if (updateOf != null && updateOf.Count() > 0)
            {                
                foreach (var propertyInfo in updateOf.GetPropertiesInfos())
                {
                    var attribute = propertyInfo.GetCustomAttribute(typeof(ColumnAttribute));
                    if (attribute != null)
                    {
                        var dbColumnName = ((ColumnAttribute) attribute).Name;
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