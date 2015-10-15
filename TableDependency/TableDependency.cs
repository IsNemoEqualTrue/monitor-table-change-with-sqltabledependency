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
using TableDependency.Mappers;

namespace TableDependency
{
    public abstract class TableDependency<T> : ITableDependency<T>, IDisposable where T : class
    {
        #region Private variables

        protected CancellationTokenSource _cancellationTokenSource;
        protected string _dataBaseObjectsNamingConvention;
        protected bool _automaticDatabaseObjectsTeardown;
        protected bool _needsToCreateDatabaseObjects;
        protected ModelToTableMapper<T> _mapper;
        protected string _connectionString;
        protected string _tableName;
        protected Task _task;

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

        #region Protected methods

        protected abstract void DropDatabaseObjects(string connectionString, string dataBaseObjectsNamingConvention);

        protected string TableNameFromModel()
        {
            return (from attribute in Attribute.GetCustomAttributes(typeof (T)) where attribute.GetType().Name == "Table" select ((TableAttribute) attribute).Name).FirstOrDefault();
        }

        protected ModelToTableMapper<T> ColumnNamesFromModel()
        {
            var columnAttributeList = typeof(T)
                .GetProperties(BindingFlags.GetProperty | BindingFlags.Instance | BindingFlags.Public)
                .Where(x => Attribute.IsDefined(x, typeof(ColumnAttribute), false))
                .ToArray();

            if (columnAttributeList.Any())
            {
                var mapper = new ModelToTableMapper<T>();
                foreach (var columnAttribute in columnAttributeList)
                {
                    mapper.AddMapping(columnAttribute, columnAttribute.Name);
                }
            }

            return null;
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