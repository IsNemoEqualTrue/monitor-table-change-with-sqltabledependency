using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Linq;
using System.Security.Permissions;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using TableDependency.Delegates;
using TableDependency.EventArgs;
using TableDependency.Exceptions;
using TableDependency.Extensions;
using TableDependency.Mappers;
using TableDependency.SqlClient.EventArgs;
using TableDependency.SqlClient.Exceptions;
using TableDependency.SqlClient.MessageTypes;
using TableDependency.SqlClient.Resources;

namespace TableDependency.SqlClient
{
    /// <summary>
    /// SqlTableDependency class.
    /// </summary>
    /// <remarks>
    /// SQL Server version 2012
    /// .NET 4.5
    /// </remarks>
    public class SqlTableDependency<T> : ITableDependency<T>, IDisposable where T : class
    {
        #region Private variables

        private Task _task;
        private CancellationTokenSource _cancellationTokenSource;
        private readonly string _dataBaseObjectsNamingConvention;
        private readonly ModelToTableMapper<T> _modelToTableMapper;
        private readonly string _connectionString;
        private Guid _dialogHandle = Guid.Empty;
        private bool _disposed;
        private readonly string _tableName;
        private readonly IList<string> _processableMessages;
        private readonly string _tableColumns;
        private readonly string _selectColumns;
        private readonly string _updateColumns;

        #endregion

        #region Properties

        /// <summary>
        /// Return the database objects naming convention. 
        /// </summary>
        /// <value>
        /// The data base objects naming.
        /// </value>
        public string DataBaseObjectsNamingConvention => string.Copy(this._dataBaseObjectsNamingConvention);

        #endregion

        #region Events

        /// <summary>
        /// Occurs when an error happen during listening changes on monitored table.
        /// </summary>
        public event ErrorEventHandler OnError;

        /// <summary>
        /// Occurs when the table content has been changed with an update, insert or delete operation.
        /// </summary>
        public event ChangedEventHandler<T> OnChanged;

        #endregion

        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="modelToColumnsTableMapper">The model to columns table mapper.</param>
        /// <param name="columnsToMonitorDuringUpdate">List containing column names for which the trigger should insert a message in the queue when its value is updated.</param>
        public SqlTableDependency(
            string connectionString,
            string tableName,
            ModelToTableMapper<T> modelToColumnsTableMapper = null,
            IEnumerable<string> columnsToMonitorDuringUpdate = null)
        {
            PreliminaryChecks(connectionString, tableName);

            _modelToTableMapper = modelToColumnsTableMapper;
            _connectionString = connectionString;
            _tableName = tableName;
            this._dataBaseObjectsNamingConvention = $"{tableName}_{Guid.NewGuid()}";

            var tableColumnsList = GetTableColumnsAndCheckForAdmittedTypes(connectionString, tableName);
            var columnsList = tableColumnsList as Tuple<string, string, int>[] ?? tableColumnsList.ToArray();
            if (columnsList.Length == 0) throw new NoColumnsException(tableName);

            if (_modelToTableMapper != null) CheckModelToTableMapperConsistency(columnsList);

            _tableColumns = string.Join(", ", columnsList.Select(c => (c.Item3 > 0) ? $"[{c.Item1}] {c.Item2.ToUpper()}({c.Item3.ToString()})" : $"[{c.Item1}] {c.Item2.ToUpper()}").ToList());
            _selectColumns = string.Join(", ", columnsList.Select(c => $"[{c.Item1}]").ToList());
            _updateColumns = columnsToMonitorDuringUpdate != null ? GetTableColumnsUpdate(columnsList, columnsToMonitorDuringUpdate) : null;

            _processableMessages = new List<string>()
            {
                string.Format(CustomMessageTypes.TemplateUpdatedMessageType, this._dataBaseObjectsNamingConvention),
                string.Format(CustomMessageTypes.TemplateDeletedMessageType, this._dataBaseObjectsNamingConvention),
                string.Format(CustomMessageTypes.TemplateInsertedMessageType, this._dataBaseObjectsNamingConvention)
            };
        }

        #endregion

        #region Public methods

        /// <summary>
        /// Starts monitoring change in content's table.
        /// </summary>
        /// <param name="timeOut">The WAIT FOR timeout in seconds.</param>
        /// <param name="watchDogTimeOut">The watchDog timeout in seconds.</param>
        /// <returns></returns>
        /// <exception cref="NoSubscriberException"></exception>
        /// <exception cref="TableDependency.Exceptions.NoSubscriberException"></exception>
        public void Start(int timeOut = 120, int watchDogTimeOut = 180)
        {
            if (timeOut < 60) throw new ArgumentException("timeOut must be great or equal to 60 seconds");
            if (watchDogTimeOut < 60 || watchDogTimeOut < (timeOut + 60)) throw new ArgumentException("watchDogTimeOut must be at least 60 seconds bigger then timeOut");

            if (_task != null)
            {
                Debug.WriteLine("SqlTableDependency: Already called Start() method.");
                return;
            }

            if (OnChanged == null) throw new NoSubscriberException();
            var onChangedSubscribedList = OnChanged.GetInvocationList();
            var onErrorSubscribedList = OnError?.GetInvocationList();

            CreateDatabaseObjects(_connectionString, _tableName, this._dataBaseObjectsNamingConvention, _tableColumns, _selectColumns, _updateColumns);
            _dialogHandle = GetConversationHandle(_connectionString, this._dataBaseObjectsNamingConvention);

            _cancellationTokenSource = new CancellationTokenSource();
            _task = Task.Factory.StartNew(() => WaitForNotification(_cancellationTokenSource.Token, onChangedSubscribedList, onErrorSubscribedList, _connectionString, this._dataBaseObjectsNamingConvention, _dialogHandle, timeOut, watchDogTimeOut, _processableMessages, _modelToTableMapper), _cancellationTokenSource.Token);

            Debug.WriteLine("SqlTableDependency: Started waiting for notification.");
        }

        /// <summary>
        /// Stops monitoring change in the table contents.
        /// </summary>
        public void Stop()
        {
            if (_task != null)
            {
                _cancellationTokenSource.Cancel(true);
                _task.Wait();
            }

            _task = null;

            DropDatabaseObjects(_connectionString, this._dataBaseObjectsNamingConvention);

            _disposed = true;

            Debug.WriteLine("SqlTableDependency: Stopped waiting for notification.");
        }

        #endregion

        #region Private methods

        private async static Task WaitForNotification(CancellationToken cancellationToken, Delegate[] onChangeSubscribedList, Delegate[] onErrorSubscribedList, string connectionString, string databaseObjectsNaming, Guid dialogHandle, int timeOut, int timeOutWatchDog, ICollection<string> processableMessages, ModelToTableMapper<T> modelMapper)
        {
            var waitForStatement = string.Format(
                "BEGIN CONVERSATION TIMER ('{0}') TIMEOUT = {3}; " +
                "WAITFOR(RECEIVE TOP (1) [conversation_handle], [message_type_name], CONVERT(XML, message_body) AS message_body FROM [{1}]), TIMEOUT {2};",
                dialogHandle,
                databaseObjectsNaming,
                timeOut * 1000,
                timeOutWatchDog);

            try
            {
                using (var sqlConnection = new SqlConnection(connectionString))
                {
                    sqlConnection.Open();

                    using (var sqlCommand = sqlConnection.CreateCommand())
                    {
                        sqlCommand.CommandText = waitForStatement;
                        sqlCommand.CommandTimeout = 0;
                        sqlCommand.Prepare();

                        while (true)
                        {
                            try
                            {
                                using (var sqlDataReader = await sqlCommand.ExecuteReaderAsync(cancellationToken).WithCancellation(cancellationToken))
                                {
                                    while (await sqlDataReader.ReadAsync(cancellationToken))
                                    {
                                        var messageType = sqlDataReader.GetSqlString(1);
                                        if (messageType.Value == SqlMessageTypes.EndDialogType || messageType.Value == SqlMessageTypes.ErrorType)
                                        {
                                            SendEndConversationMessage(sqlConnection, sqlDataReader.GetSqlGuid(0));

                                            if (messageType.Value == SqlMessageTypes.ErrorType) throw new ServiceBrokerErrorMessageException(databaseObjectsNaming);
                                            throw new ServiceBrokerEndDialogException(databaseObjectsNaming);
                                        }
                                        if (processableMessages.Contains(messageType.Value))
                                        {
                                            RaiseEvent(onChangeSubscribedList, databaseObjectsNaming, messageType.Value, modelMapper, sqlDataReader.GetSqlXml(2));
                                        }
                                    }
                                }
                            }
                            catch (Exception exception)
                            {
                                ThrowIfSqlClientCancellationRequested(cancellationToken, exception);
                                throw;
                            }
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Debug.WriteLine("SqlTableDependency: Operation canceled.");
            }
            catch (Exception exception)
            {
                Debug.WriteLine("SqlTableDependency: Exception " + exception.Message + ".");
                if (cancellationToken.IsCancellationRequested == false) NotifyListenersAboutError(onErrorSubscribedList, exception);
            }
        }

        private static void NotifyListenersAboutError(Delegate[] onErrorSubscribedList, Exception exception)
        {
            if (onErrorSubscribedList != null)
            {
                foreach (var dlg in onErrorSubscribedList.Where(d => d != null))
                {
                    dlg.Method.Invoke(dlg.Target, new object[] { null, new ErrorEventArgs(exception) });
                }
            }
        }

        private static void ThrowIfSqlClientCancellationRequested(CancellationToken cancellationToken, Exception exception)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                var sqlException = exception as SqlException;
                if (null == sqlException)
                {
                    var aggregateException = exception as AggregateException;
                    if (aggregateException != null) sqlException = aggregateException.InnerException as SqlException;
                    if (sqlException == null) return;
                }

                // Assume that if it's a "real" problem (e.g. the query is malformed), then this will be a number != 0, typically from the "sysmessages" system table 
                if (sqlException.Number != 0) return;

                throw new OperationCanceledException();
            }
        }

        private static Guid GetConversationHandle(string connectionString, string databaseObjectsNaming)
        {
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    var sqlParameter = new SqlParameter { ParameterName = "@handle", DbType = DbType.Guid, Direction = ParameterDirection.Output };

                    sqlCommand.CommandText = string.Format("BEGIN DIALOG CONVERSATION @handle FROM SERVICE [{0}] TO SERVICE '{0}', 'CURRENT DATABASE' ON CONTRACT [{0}] WITH ENCRYPTION = OFF;", databaseObjectsNaming);
                    sqlCommand.Parameters.Add(sqlParameter);
                    sqlCommand.ExecuteNonQuery();
                    var dialogHandle = (Guid)sqlParameter.Value;

                    return dialogHandle;
                }
            }
        }

        private static void SendEndConversationMessage(SqlConnection sqlConnection, SqlGuid handle)
        {
            using (var sqlCommand = sqlConnection.CreateCommand())
            {
                sqlCommand.CommandText = "END CONVERSATION @handle";
                sqlCommand.Parameters.Add("@handle", SqlDbType.UniqueIdentifier);
                sqlCommand.Parameters["@handle"].Value = handle;
                sqlCommand.ExecuteNonQuery();
            }
        }

        private static void RaiseEvent(IEnumerable<Delegate> delegates, string databaseObjectsNaming, string messageType, ModelToTableMapper<T> modelMapper, SqlXml message = null)
        {
            if (delegates == null) return;

            foreach (var dlg in delegates.Where(d => d != null))
            {
                dlg.Method.Invoke(dlg.Target, message != null && message.IsNull == false
                        ? new object[] { null, new SqlRecordChangedEventArgs<T>(databaseObjectsNaming, messageType, message, modelMapper) }
                        : new object[] { null, new SqlRecordChangedEventArgs<T>(messageType) });
            }
        }

        private static IEnumerable<Tuple<string, string, int>> GetTableColumnsAndCheckForAdmittedTypes(string connectionString, string tableName)
        {
            var columnsList = new List<Tuple<string, string, int>>();

            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"SELECT COLUMN_NAME, DATA_TYPE, ISNULL(CHARACTER_MAXIMUM_LENGTH, 0) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableName}' ORDER BY ORDINAL_POSITION";
                    var reader = sqlCommand.ExecuteReader();
                    while (reader.Read())
                    {
                        var name = reader.GetString(0);
                        var type = reader.GetString(1);
                        var size = reader.GetInt32(2);

                        switch (type.ToLower())
                        {
                            case "xml":
                            case "table":
                            case "text":
                            case "ntext":
                            case "image":
                            case "binary":
                            case "varbinary":
                            case "sql_variant":
                                throw new ColumnTypeNotSupportedException(tableName, type);
                        }

                        columnsList.Add(new Tuple<string, string, int>(name, type, size));
                    }
                }
            }

            return columnsList;
        }

        private static void CreateDatabaseObjects(string connectionString, string tableName, string databaseObjectsNaming, string tableColumns, string selectColumns, string updateColumns)
        {
            using (var transactionScope = new TransactionScope(TransactionScopeOption.RequiresNew))
            {
                using (var sqlConnection = new SqlConnection(connectionString))
                {
                    sqlConnection.Open();
                    using (var sqlCommand = sqlConnection.CreateCommand())
                    {
                        sqlCommand.CommandText = $"SELECT COUNT(*) FROM sys.service_queues WHERE name = N'{databaseObjectsNaming}'";
                        if (((int)sqlCommand.ExecuteScalar()) > 0) throw new QueueAlreadyUsedException(databaseObjectsNaming);

                        sqlCommand.CommandText = string.Format(Scripts.CreateMessageDeleted, databaseObjectsNaming);
                        sqlCommand.ExecuteNonQuery();

                        sqlCommand.CommandText = string.Format(Scripts.CreateMessageInserted, databaseObjectsNaming);
                        sqlCommand.ExecuteNonQuery();

                        sqlCommand.CommandText = string.Format(Scripts.CreateMessageUpdated, databaseObjectsNaming);
                        sqlCommand.ExecuteNonQuery();

                        sqlCommand.CommandText = string.Format(Scripts.CreateContract, databaseObjectsNaming);
                        sqlCommand.ExecuteNonQuery();

                        var dropAllScript = string.Format(Scripts.ScriptDropAll, databaseObjectsNaming);
                        sqlCommand.CommandText = string.Format(Scripts.CreateProcedureQueueActivation, databaseObjectsNaming, dropAllScript);
                        sqlCommand.ExecuteNonQuery();

                        sqlCommand.CommandText = string.Format(Scripts.CreateQueue, databaseObjectsNaming);
                        sqlCommand.ExecuteNonQuery();

                        sqlCommand.CommandText = string.Format(Scripts.CreateService, databaseObjectsNaming);
                        sqlCommand.ExecuteNonQuery();

                        var bodyForUpdate = !string.IsNullOrEmpty(updateColumns) ? string.Format(Scripts.TriggerUpdateWithColumns, updateColumns, tableName) : string.Format(Scripts.TriggerUpdateWithoutColuns, tableName);
                        sqlCommand.CommandText = string.Format(Scripts.CreateTrigger, databaseObjectsNaming, tableName, tableColumns, selectColumns, bodyForUpdate);
                        sqlCommand.ExecuteNonQuery();
                    }
                }

                transactionScope.Complete();
            }

            Debug.WriteLine($"SqlTableDependency: Database objects created with naming {databaseObjectsNaming}.");
        }

        private static void DropDatabaseObjects(string connectionString, string databaseObjectsNaming)
        {
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandText = string.Format(Scripts.ScriptDropAll, databaseObjectsNaming);
                    sqlCommand.ExecuteNonQuery();
                }
            }

            Debug.WriteLine("SqlTableDependency: Database objects destroyed.");
        }

        private static void PreliminaryChecks(string connectionString, string tableName)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException(nameof(connectionString));
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException(nameof(tableName));

            CheckIfConnectionStringIsValid(connectionString);
            CheckIfUserHasPermission();

            using (var sqlConnection = new SqlConnection(connectionString))
            {
                try
                {
                    sqlConnection.Open();
                }
                catch (SqlException exception)
                {
                    throw new InvalidConnectionStringException(exception);
                }

                CheckIfServiceBrokerIsEnabled(sqlConnection);
                CheckIfTableExists(sqlConnection, tableName);
            }
        }

        private static void CheckIfConnectionStringIsValid(string connectionString)
        {
            try
            {
                new SqlConnectionStringBuilder(connectionString);
            }
            catch (Exception exception)
            {
                throw new InvalidConnectionStringException(exception);
            }
        }

        private static void CheckIfUserHasPermission()
        {
            try
            {
                new SqlClientPermission(PermissionState.Unrestricted).Demand();
            }
            catch (Exception exception)
            {
                throw new UserWithNoPermissionException(exception);
            }
        }

        private static void CheckIfServiceBrokerIsEnabled(SqlConnection sqlConnection)
        {
            using (var sqlCommand = sqlConnection.CreateCommand())
            {
                sqlCommand.CommandText = "SELECT is_broker_enabled FROM sys.databases WHERE database_id = db_id()";
                if ((bool)sqlCommand.ExecuteScalar() == false) throw new ServiceBrokerNotEnabledException();
            }
        }

        private static void CheckIfTableExists(SqlConnection sqlConnection, string tableName)
        {
            using (var sqlCommand = sqlConnection.CreateCommand())
            {
                sqlCommand.CommandText = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tableName}'";
                if ((int)sqlCommand.ExecuteScalar() == 0) throw new NotExistingTableException(tableName);
            }
        }

        private void CheckModelToTableMapperConsistency(IEnumerable<Tuple<string, string, int>> tableColumnsList)
        {
            if (_modelToTableMapper.Count() < 1) throw new ModelToTableMapperException();

            var dbColumnNames = tableColumnsList.Select(t => t.Item1.ToLower()).ToList();

            if (_modelToTableMapper.GetMappings().Select(t => t.Value).Any(mappingColumnName => !dbColumnNames.Contains(mappingColumnName.ToLower())))
            {
                throw new ModelToTableMapperException();
            }
        }

        private string GetTableColumnsUpdate(IEnumerable<Tuple<string, string, int>> tableColumnsList, IEnumerable<string> columnsUpdateMonitoring)
        {
            var columnsToMonitorDuringUpdate = columnsUpdateMonitoring as string[] ?? columnsUpdateMonitoring.ToArray();
            if (!columnsToMonitorDuringUpdate.Any()) throw new UpdateOfException("columnsToMonitorDuringUpdate parameter is empty.");

            if (columnsToMonitorDuringUpdate.Any(string.IsNullOrWhiteSpace))
            {
                throw new UpdateOfException("columnsToMonitorDuringUpdate parameter contains a null or empty value.");
            }

            var dbColumnNames = tableColumnsList.Select(t => t.Item1.ToLower()).ToList();
            foreach (var columnToMonitorDuringUpdate in columnsToMonitorDuringUpdate.Where(columnToMonitor => !dbColumnNames.Contains(columnToMonitor.ToLower())))
            {
                throw new UpdateOfException($"Column {columnToMonitorDuringUpdate} does not exists");
            }

            return string.Join(" OR ", columnsToMonitorDuringUpdate.Where(c => !string.IsNullOrWhiteSpace(c)).Distinct(StringComparer.CurrentCultureIgnoreCase).Select(c => $"UPDATE([{c}])").ToList());
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

        ~SqlTableDependency()
        {
            Dispose(false);
        }

        #endregion
    }
}