////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
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
using TableDependency.Enums;
using TableDependency.Exceptions;
using TableDependency.Extensions;
using TableDependency.Mappers;
using TableDependency.Messages;
using TableDependency.SqlClient.Extensions;
using TableDependency.SqlClient.EventArgs;
using TableDependency.SqlClient.Exceptions;
using TableDependency.SqlClient.Messages;
using TableDependency.SqlClient.Resources;
using TableDependency.Utilities;
using ErrorEventArgs = TableDependency.EventArgs.ErrorEventArgs;
using ErrorEventHandler = TableDependency.Delegates.ErrorEventHandler;

namespace TableDependency.SqlClient
{
    /// <summary>
    /// SqlTableDependency class.
    /// </summary>
    public class SqlTableDependency<T> : ITableDependency<T>, IDisposable where T : class
    {
        #region Private variables

        private const string EndMessageTemplate = "{0}/EndDialog";
        private const string StartMessageTemplate = "{0}/StartDialog";
        private const string Max = "MAX";

        private Task _task;
        private CancellationTokenSource _cancellationTokenSource;
        private readonly bool _needsToCreateDatabaseObjects;
        private readonly string _dataBaseObjectsNamingConvention;
        private readonly bool _automaticDatabaseObjectsTeardown;
        private readonly ModelToTableMapper<T> _mapper;
        private readonly string _connectionString;
        private Guid _dialogHandle = Guid.Empty;
        private bool _disposed;
        private readonly string _tableName;
        private readonly IEnumerable<Tuple<string, SqlDbType, string>> _userInterestedColumns;
        private readonly IEnumerable<string> _updateOf;

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
        public TableDependencyStatus Status { get; private set; }

        #endregion

        #region Events

        /// <summary>
        /// Occurs when an error happen during listening for changes on monitored table.
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
        /// <param name="mapper">The model to column table mapper.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        /// <param name="automaticDatabaseObjectsTeardown">Destroy all database objects created for receive notifications.</param>
        /// <param name="namingConventionForDatabaseObjects">The naming convention for database objects.</param>
        public SqlTableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper = null, IEnumerable<string> updateOf = null, bool automaticDatabaseObjectsTeardown = true, string namingConventionForDatabaseObjects = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException(nameof(connectionString));
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException(nameof(tableName));

            PreliminaryChecks(connectionString, tableName, null);

            _dataBaseObjectsNamingConvention = namingConventionForDatabaseObjects ?? $"{tableName}_{Guid.NewGuid()}";
            _connectionString = connectionString;
            _tableName = tableName;
            _mapper = mapper;
            _updateOf = updateOf;
            _automaticDatabaseObjectsTeardown = automaticDatabaseObjectsTeardown;
            _userInterestedColumns = GetColumnsToUseForCreatingDbObjects(_updateOf);
            _needsToCreateDatabaseObjects = CheckIfNeedsToCreateDatabaseObjects();

            Status = TableDependencyStatus.WaitingToStart;
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
        public void Start(int timeOut = 120, int watchDogTimeOut = 180)
        {
            if (timeOut < 60) throw new ArgumentException("timeOut must be greater or equal to 60 seconds");
            if (watchDogTimeOut < 60 || watchDogTimeOut < (timeOut + 60)) throw new ArgumentException("watchDogTimeOut must be at least 60 seconds bigger then timeOut");

            if (_task != null)
            {
                Debug.WriteLine("SqlTableDependency: Already called Start() method.");
                return;
            }

            if (OnChanged == null) throw new NoSubscriberException();
            var onChangedSubscribedList = OnChanged.GetInvocationList();
            var onErrorSubscribedList = OnError?.GetInvocationList();

            IList<string> processableMessages;
            if (_needsToCreateDatabaseObjects)
            {
                var toUseCreatingTrigger = _userInterestedColumns as Tuple<string, SqlDbType, string>[] ?? _userInterestedColumns.ToArray();
                var columnsForTableVariable = PrepareColumnListForTableVariable(toUseCreatingTrigger);
                var columnsForSelect = string.Join(", ", toUseCreatingTrigger.Select(c => $"[{c.Item1}]").ToList());
                var columnsForUpdateOf = _updateOf != null ? string.Join(" OR ", _updateOf.Where(c => !string.IsNullOrWhiteSpace(c)).Distinct(StringComparer.CurrentCultureIgnoreCase).Select(c => $"UPDATE([{c}])").ToList()) : null;
                processableMessages = CreateDatabaseObjects(_connectionString, _tableName, _userInterestedColumns, _dataBaseObjectsNamingConvention, columnsForTableVariable, columnsForSelect, columnsForUpdateOf);
            }
            else
            {
                processableMessages = RetrieveProcessableMessages(_userInterestedColumns, _dataBaseObjectsNamingConvention);
            }

            _dialogHandle = GetConversationHandle(_connectionString, _dataBaseObjectsNamingConvention);

            _cancellationTokenSource = new CancellationTokenSource();
            _task = Task.Factory.StartNew(() =>
                WaitForNotification(
                    _cancellationTokenSource.Token,
                    onChangedSubscribedList,
                    onErrorSubscribedList,
                    OnStatusChanged,
                    _connectionString,
                    _dataBaseObjectsNamingConvention,
                    _dialogHandle,
                    timeOut,
                    watchDogTimeOut,
                    processableMessages,
                    _mapper,
                    _automaticDatabaseObjectsTeardown),
                _cancellationTokenSource.Token);

            this.Status = TableDependencyStatus.Starting;
            Debug.WriteLine("SqlTableDependency: Started waiting for notification.");
        }

        /// <summary>
        /// Stops monitoring table's content changes.
        /// </summary>
        public void Stop()
        {
            if (_task != null)
            {
                _cancellationTokenSource.Cancel(true);
                _task?.Wait();
            }

            _task = null;

            if (_automaticDatabaseObjectsTeardown) DropDatabaseObjects(_connectionString, _dataBaseObjectsNamingConvention, _userInterestedColumns);

            _disposed = true;

            Debug.WriteLine("SqlTableDependency: Stopped waiting for notification.");
        }

        #endregion

        #region Private methods

        private async static Task WaitForNotification(
            CancellationToken cancellationToken,
            Delegate[] onChangeSubscribedList,
            Delegate[] onErrorSubscribedList,
            Action<TableDependencyStatus> setStatus,
            string connectionString,
            string databaseObjectsNaming,
            Guid dialogHandle,
            int timeOut,
            int timeOutWatchDog,
            ICollection<string> processableMessages,
            ModelToTableMapper<T> modelMapper,
            bool automaticDatabaseObjectsTeardown)
        {
            setStatus(TableDependencyStatus.Started);

            var messagesBag = new MessagesBag(string.Format(StartMessageTemplate, databaseObjectsNaming), string.Format(EndMessageTemplate, databaseObjectsNaming));

            var waitForStatement = automaticDatabaseObjectsTeardown
                ? $"BEGIN CONVERSATION TIMER ('{dialogHandle}') TIMEOUT = {timeOutWatchDog}; WAITFOR(RECEIVE TOP (1) [conversation_handle], [message_type_name], [message_body] FROM [{databaseObjectsNaming}]), TIMEOUT {(timeOut * 1000)};"
                : $"WAITFOR(RECEIVE TOP (1) [conversation_handle], [message_type_name], [message_body] FROM [{databaseObjectsNaming}]), TIMEOUT {(timeOut * 1000)};";

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
                                setStatus(TableDependencyStatus.ListenerForNotification);
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
                                            var messageContent = sqlDataReader.IsDBNull(2) ? null : sqlDataReader.GetSqlBytes(2).Value;
                                            var messageStatus = messagesBag.AddMessage(messageType.Value, messageContent);
                                            if (messageStatus == MessagesBagStatus.Closed) RaiseEvent(onChangeSubscribedList, modelMapper, messagesBag);
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
                setStatus(TableDependencyStatus.StoppedDueToCancellation);
            }
            catch (Exception exception)
            {
                Debug.WriteLine("SqlTableDependency: Exception " + exception.Message + ".");
                setStatus(TableDependencyStatus.StoppedDueToError);
                if (cancellationToken.IsCancellationRequested == false) NotifyListenersAboutError(onErrorSubscribedList, exception);
            }
        }

        private bool CheckIfNeedsToCreateDatabaseObjects()
        {
            IList<bool> allObjectAlreadyPresent = new List<bool>();

            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"SELECT COUNT(*) FROM SYS.TRIGGERS WHERE NAME = 'tr_{_dataBaseObjectsNamingConvention}'";
                    allObjectAlreadyPresent.Add((int)sqlCommand.ExecuteScalar() > 0);

                    sqlCommand.CommandText = $"SELECT COUNT(*) FROM SYS.OBJECTS WHERE name = N'{_dataBaseObjectsNamingConvention}_QueueActivation'";
                    allObjectAlreadyPresent.Add((int)sqlCommand.ExecuteScalar() > 0);

                    sqlCommand.CommandText = $"SELECT COUNT(*) FROM SYS.SERVICES WHERE NAME = N'{_dataBaseObjectsNamingConvention}'";
                    allObjectAlreadyPresent.Add((int)sqlCommand.ExecuteScalar() > 0);

                    sqlCommand.CommandText = $"SELECT COUNT(*) FROM SYS.SERVICE_QUEUES WHERE NAME = N'{_dataBaseObjectsNamingConvention}'";
                    allObjectAlreadyPresent.Add((int)sqlCommand.ExecuteScalar() > 0);

                    sqlCommand.CommandText = $"SELECT COUNT(*) FROM SYS.SERVICE_CONTRACTS WHERE name = N'{_dataBaseObjectsNamingConvention}'";
                    allObjectAlreadyPresent.Add((int)sqlCommand.ExecuteScalar() > 0);

                    sqlCommand.CommandText = "SELECT COUNT(*) FROM SYS.SERVICE_MESSAGE_TYPES WHERE name = N'" + string.Format(StartMessageTemplate, _dataBaseObjectsNamingConvention) + "'";
                    allObjectAlreadyPresent.Add((int)sqlCommand.ExecuteScalar() > 0);

                    sqlCommand.CommandText = "SELECT COUNT(*) FROM SYS.SERVICE_MESSAGE_TYPES WHERE name = N'" + string.Format(EndMessageTemplate, _dataBaseObjectsNamingConvention) + "'";
                    allObjectAlreadyPresent.Add((int)sqlCommand.ExecuteScalar() > 0);

                    foreach (var userInterestedColumn in _userInterestedColumns)
                    {
                        sqlCommand.CommandText = "SELECT COUNT(*) FROM SYS.SERVICE_MESSAGE_TYPES WHERE name = N'" + $"{_dataBaseObjectsNamingConvention}/{ChangeType.Delete}/{userInterestedColumn.Item1}" + "'";
                        allObjectAlreadyPresent.Add((int)sqlCommand.ExecuteScalar() > 0);

                        sqlCommand.CommandText = "SELECT COUNT(*) FROM SYS.SERVICE_MESSAGE_TYPES WHERE name = N'" + $"{_dataBaseObjectsNamingConvention}/{ChangeType.Insert}/{userInterestedColumn.Item1}" + "'";
                        allObjectAlreadyPresent.Add((int)sqlCommand.ExecuteScalar() > 0);

                        sqlCommand.CommandText = "SELECT COUNT(*) FROM SYS.SERVICE_MESSAGE_TYPES WHERE name = N'" + $"{_dataBaseObjectsNamingConvention}/{ChangeType.Update}/{userInterestedColumn.Item1}" + "'";
                        allObjectAlreadyPresent.Add((int)sqlCommand.ExecuteScalar() > 0);
                    }
                }
            }

            if (allObjectAlreadyPresent.All(exist => exist == false)) return true;
            if (allObjectAlreadyPresent.All(exist => exist == true)) return false;

            // Not all objects are present
            throw new SomeDatabaseObjectsNotPresentException(_dataBaseObjectsNamingConvention);
        }

        private void OnStatusChanged(TableDependencyStatus status)
        {
            this.Status = status;
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

        private static string PrepareColumnListForTableVariable(IEnumerable<Tuple<string, SqlDbType, string>> tableColumns)
        {
            var columns = tableColumns.Select(tableColumn =>
            {
                if (tableColumn.Item2 == SqlDbType.Timestamp)
                {
                    return $"[{tableColumn.Item1}] {SqlDbType.Binary}(8)";
                }

                if (!string.IsNullOrWhiteSpace(tableColumn.Item3))
                {
                    return $"[{tableColumn.Item1}] {tableColumn.Item2}({tableColumn.Item3})";
                }

                return $"[{tableColumn.Item1}] {tableColumn.Item2}";
            });

            return string.Join(", ", columns.ToList());
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

        private static void RaiseEvent(IEnumerable<Delegate> delegates, ModelToTableMapper<T> modelMapper, MessagesBag messagesBag)
        {
            if (delegates == null) return;
            foreach (var dlg in delegates.Where(d => d != null)) dlg.Method.Invoke(dlg.Target, new object[] { null, new SqlRecordChangedEventArgs<T>(messagesBag, modelMapper) });
        }

        private static string ComputeSize(SqlDbType dataType, string characterMaximumLength, string numericPrecision, string numericScale, string dateTimePrecisione)
        {
            switch (dataType)
            {                               
                case SqlDbType.Binary:
                case SqlDbType.VarBinary:
                case SqlDbType.Char:
                case SqlDbType.NChar:
                case SqlDbType.VarChar:
                case SqlDbType.NVarChar:
                    return characterMaximumLength == "-1" ? Max : characterMaximumLength;

                case SqlDbType.Decimal:
                    return $"{numericPrecision},{numericScale}";

                case SqlDbType.DateTime2:
                case SqlDbType.DateTimeOffset:
                case SqlDbType.Time:
                    return $"{dateTimePrecisione}";

                default:
                    return null;
            }
        }

        private static IEnumerable<Tuple<string, SqlDbType, string>> GetTableColumnsList(string connectionString, string tableName)
        {
            var columnsList = new List<Tuple<string, SqlDbType, string>>();

            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableName}' ORDER BY ORDINAL_POSITION";
                    var reader = sqlCommand.ExecuteReader();
                    while (reader.Read())
                    {
                        var name = reader.GetString(0);
                        var type = reader.GetString(1).ToSqlDbType();
                        var size = ComputeSize(type, reader.GetSafeString(2), reader.GetSafeString(3), reader.GetSafeString(4), reader.GetSafeString(5));
                        columnsList.Add(new Tuple<string, SqlDbType, string>(name, type, size));
                    }
                }
            }

            return columnsList;
        }

        private IEnumerable<Tuple<string, SqlDbType, string>> GetColumnsToUseForCreatingDbObjects(IEnumerable<string> updateOf = null)
        {
            var tableColumns = GetTableColumnsList(_connectionString, _tableName);
            var tableColumnsList = tableColumns as Tuple<string, SqlDbType, string>[] ?? tableColumns.ToArray();
            if (!tableColumnsList.Any()) throw new NoColumnsException(_tableName);

            CheckUpdateOfValidity(tableColumnsList, updateOf);
            CheckMapperValidity(tableColumnsList);

            var userIterestedColumns = GetUserInterestedColumns(tableColumnsList);

            var columnsToUseForCreatingDbObjects = userIterestedColumns as Tuple<string, SqlDbType, string>[] ?? userIterestedColumns.ToArray();
            CheckIfUserInterestedColumnsCanBeManaged(columnsToUseForCreatingDbObjects);
            return columnsToUseForCreatingDbObjects;
        }

        private void CheckMapperValidity(IEnumerable<Tuple<string, SqlDbType, string>> tableColumnsList)
        {
            if (_mapper != null)
            {
                if (_mapper.Count() < 1) throw new ModelToTableMapperException();

                var dbColumnNames = tableColumnsList.Select(t => t.Item1.ToLower()).ToList();

                if (_mapper.GetMappings().Select(t => t.Value).Any(mappingColumnName => !dbColumnNames.Contains(mappingColumnName.ToLower())))
                {
                    throw new ModelToTableMapperException();
                }
            }
        }

        private void CheckIfUserInterestedColumnsCanBeManaged(IEnumerable<Tuple<string, SqlDbType, string>> tableColumnsToUse)
        {
            foreach (var tableColumn in tableColumnsToUse)
            {
                switch (tableColumn.Item2)
                {
                    case SqlDbType.Image:
                    case SqlDbType.Text:
                    case SqlDbType.NText:
                    case SqlDbType.Structured:
                    case SqlDbType.Udt:
                    case SqlDbType.Variant:
                        throw new ColumnTypeNotSupportedException($"{tableColumn.Item2} type is not an admitted for SqlTableDependency.");
                }
            }
        }

        private IList<string> RetrieveProcessableMessages(IEnumerable<Tuple<string, SqlDbType, string>> userInterestedColumns, string databaseObjectsNaming)
        {
            var processableMessages = new List<string>
            {
                string.Format(StartMessageTemplate, databaseObjectsNaming),
                string.Format(EndMessageTemplate, databaseObjectsNaming)
            };

            var interestedColumns = userInterestedColumns as Tuple<string, SqlDbType, string>[] ?? userInterestedColumns.ToArray();
            foreach (var userInterestedColumn in interestedColumns)
            {
                processableMessages.Add($"{databaseObjectsNaming}/{ChangeType.Delete}/{userInterestedColumn.Item1}");
                processableMessages.Add($"{databaseObjectsNaming}/{ChangeType.Insert}/{userInterestedColumn.Item1}");
                processableMessages.Add($"{databaseObjectsNaming}/{ChangeType.Update}/{userInterestedColumn.Item1}");
            }

            return processableMessages;
        }

        private static IList<string> CreateDatabaseObjects(string connectionString, string tableName, IEnumerable<Tuple<string, SqlDbType, string>> userInterestedColumns, string databaseObjectsNaming, string tableColumns, string selectColumns, string updateColumns)
        {
            var processableMessages = new List<string>();

            using (var transactionScope = new TransactionScope(TransactionScopeOption.RequiresNew))
            {
                using (var sqlConnection = new SqlConnection(connectionString))
                {
                    sqlConnection.Open();
                    using (var sqlCommand = sqlConnection.CreateCommand())
                    {
                        sqlCommand.CommandText = $"SELECT COUNT(*) FROM sys.service_queues WHERE name = N'{databaseObjectsNaming}'";
                        if (((int)sqlCommand.ExecuteScalar()) > 0) throw new QueueAlreadyUsedException(databaseObjectsNaming);

                        var startMessage = string.Format(StartMessageTemplate, databaseObjectsNaming);
                        var endMessage = string.Format(EndMessageTemplate, databaseObjectsNaming);

                        sqlCommand.CommandText = $"CREATE MESSAGE TYPE[{startMessage}] VALIDATION = NONE; " + Environment.NewLine + $"CREATE MESSAGE TYPE[{endMessage}] VALIDATION = NONE";
                        sqlCommand.ExecuteNonQuery();

                        processableMessages.Add(startMessage);
                        processableMessages.Add(endMessage);

                        var interestedColumns = userInterestedColumns as Tuple<string, SqlDbType, string>[] ?? userInterestedColumns.ToArray();
                        foreach (var userInterestedColumn in interestedColumns)
                        {
                            var deleteMessage = $"{databaseObjectsNaming}/{ChangeType.Delete}/{userInterestedColumn.Item1}";
                            var insertMessage = $"{databaseObjectsNaming}/{ChangeType.Insert}/{userInterestedColumn.Item1}";
                            var updateMessage= $"{databaseObjectsNaming}/{ChangeType.Update}/{userInterestedColumn.Item1}";
                           
                            sqlCommand.CommandText = $"CREATE MESSAGE TYPE [{deleteMessage}] VALIDATION = NONE; " + Environment.NewLine + $"CREATE MESSAGE TYPE [{insertMessage}] VALIDATION = NONE; " + Environment.NewLine + $"CREATE MESSAGE TYPE [{updateMessage}] VALIDATION = NONE;";
                            sqlCommand.ExecuteNonQuery();

                            processableMessages.Add(deleteMessage);
                            processableMessages.Add(insertMessage);
                            processableMessages.Add(updateMessage);
                        }

                        var contractBody = string.Join(", " + Environment.NewLine, processableMessages.Select(message => $"[{message}] SENT BY INITIATOR"));
                        sqlCommand.CommandText = $"CREATE CONTRACT [{databaseObjectsNaming}] ({contractBody})";
                        sqlCommand.ExecuteNonQuery();

                        var dropMessages = string.Join(Environment.NewLine, processableMessages.Select(c => string.Format("IF EXISTS (SELECT * FROM sys.service_message_types WHERE name = N'{0}') DROP MESSAGE TYPE[{0}];", c)));
                        var dropAllScript = string.Format(Scripts.ScriptDropAll, databaseObjectsNaming, dropMessages);
                        sqlCommand.CommandText = string.Format(Scripts.CreateProcedureQueueActivation, databaseObjectsNaming, dropAllScript);
                        sqlCommand.ExecuteNonQuery();
                        
                        sqlCommand.CommandText = $"CREATE QUEUE[{databaseObjectsNaming}] WITH STATUS = ON, RETENTION = OFF, POISON_MESSAGE_HANDLING (STATUS = OFF), ACTIVATION(STATUS = ON, PROCEDURE_NAME = [{databaseObjectsNaming}_QueueActivation], MAX_QUEUE_READERS = 1, EXECUTE AS OWNER)";
                        sqlCommand.ExecuteNonQuery();

                        sqlCommand.CommandText = $"CREATE SERVICE[{databaseObjectsNaming}] ON QUEUE[{databaseObjectsNaming}] ([{databaseObjectsNaming}])";
                        sqlCommand.ExecuteNonQuery();

                        var declareVariableStatement = PrepareDeclareVariableStatement(interestedColumns);
                        var selectForSetVariablesStatement = PrepareSelectForSetVarialbes(interestedColumns);
                        var sendInsertConversationStatements = PrepareSendConversation(databaseObjectsNaming, ChangeType.Insert.ToString(), interestedColumns);
                        var sendUpdatedConversationStatements = PrepareSendConversation(databaseObjectsNaming, ChangeType.Update.ToString(), interestedColumns);
                        var sendDeletedConversationStatements = PrepareSendConversation(databaseObjectsNaming, ChangeType.Delete.ToString(), interestedColumns);
                        var bodyForUpdate = !string.IsNullOrEmpty(updateColumns) ? string.Format(Scripts.TriggerUpdateWithColumns, updateColumns, tableName, selectColumns, ChangeType.Update) : string.Format(Scripts.TriggerUpdateWithoutColuns, tableName, selectColumns, ChangeType.Update);

                        sqlCommand.CommandText = string.Format(
                            Scripts.CreateTrigger,
                            databaseObjectsNaming,
                            tableName,
                            tableColumns,
                            selectColumns,
                            bodyForUpdate,
                            declareVariableStatement,
                            selectForSetVariablesStatement,
                            sendInsertConversationStatements,
                            sendUpdatedConversationStatements,
                            sendDeletedConversationStatements,
                            ChangeType.Insert,
                            ChangeType.Update,
                            ChangeType.Delete);
                        sqlCommand.ExecuteNonQuery();
                    }
                }

                transactionScope.Complete();
            }

            Debug.WriteLine($"SqlTableDependency: Database objects created with naming {databaseObjectsNaming}.");

            return processableMessages;
        }

        private static string ConvertFormat(Tuple<string, SqlDbType, string> userInterestedColumn)
        {
            return (userInterestedColumn.Item2 == SqlDbType.DateTime || userInterestedColumn.Item2 == SqlDbType.Date) ? ", 121" : string.Empty;
        }

        private static string ConvertValueByType(Tuple<string, SqlDbType, string> userInterestedColumn)
        {
            if (userInterestedColumn.Item2 == SqlDbType.Binary || userInterestedColumn.Item2 == SqlDbType.VarBinary)
            {
                return $"@{userInterestedColumn.Item1.Replace(" ", string.Empty)}";
            }

            return $"CONVERT(NVARCHAR(MAX), @{userInterestedColumn.Item1.Replace(" ", string.Empty)}{ConvertFormat(userInterestedColumn)})";
        }

        private static string PrepareSendConversation(string databaseObjectsNaming, string dmlType, IEnumerable<Tuple<string, SqlDbType, string>> userInterestedColumns)
        {
            var sendList = userInterestedColumns
                .Select(insterestedColumn => $"IF @{insterestedColumn.Item1.Replace(" ", string.Empty)} IS NOT NULL BEGIN" + Environment.NewLine + $";SEND ON CONVERSATION @h MESSAGE TYPE[{databaseObjectsNaming}/{dmlType}/{insterestedColumn.Item1}] ({ConvertValueByType(insterestedColumn)})" + Environment.NewLine + "END" + Environment.NewLine + "ELSE BEGIN" + Environment.NewLine + $";SEND ON CONVERSATION @h MESSAGE TYPE[{databaseObjectsNaming}/{dmlType}/{insterestedColumn.Item1}] (0x)" + Environment.NewLine + "END")
                .ToList();

            sendList.Insert(0, $";SEND ON CONVERSATION @h MESSAGE TYPE[{string.Format(StartMessageTemplate, databaseObjectsNaming)}] (CONVERT(NVARCHAR, @dmlType))" + Environment.NewLine);
            sendList.Add($";SEND ON CONVERSATION @h MESSAGE TYPE[{string.Format(EndMessageTemplate, databaseObjectsNaming)}] (CONVERT(NVARCHAR, @dmlType))" + Environment.NewLine);

            return string.Join(Environment.NewLine, sendList);
        }

        private static string PrepareSelectForSetVarialbes(IEnumerable<Tuple<string, SqlDbType, string>> userInterestedColumns)
        {
            return string.Join(", ", userInterestedColumns.Select(insterestedColumn => $"@{insterestedColumn.Item1.Replace(" ", string.Empty)} = [{insterestedColumn.Item1}]"));
        }

        private static string PrepareDeclareVariableStatement(IEnumerable<Tuple<string, SqlDbType, string>> userInterestedColumns)
        {
            var colonne = (from insterestedColumn in userInterestedColumns let variableName = insterestedColumn.Item1.Replace(" ", string.Empty) let variableType = $"{insterestedColumn.Item2.ToString().ToUpper()}" + (string.IsNullOrWhiteSpace(insterestedColumn.Item3) ? string.Empty : $"({insterestedColumn.Item3})") select $"DECLARE @{variableName} {variableType.ToUpper()}").ToList();
            return string.Join(Environment.NewLine, colonne);
        }

        private static void DropDatabaseObjects(string connectionString, string databaseObjectsNaming, IEnumerable<Tuple<string, SqlDbType, string>> userInterestedColumns)
        {
            var dropMessageStartEnd = new List<string>()
            {
                $"IF EXISTS (SELECT * FROM sys.service_message_types WHERE name = N'{string.Format(StartMessageTemplate, databaseObjectsNaming)}') DROP MESSAGE TYPE [{string.Format(StartMessageTemplate, databaseObjectsNaming)}];",
                $"IF EXISTS (SELECT * FROM sys.service_message_types WHERE name = N'{string.Format(EndMessageTemplate, databaseObjectsNaming)}') DROP MESSAGE TYPE [{string.Format(EndMessageTemplate, databaseObjectsNaming)}];"
            };

            var interestedColumns = userInterestedColumns as Tuple<string, SqlDbType, string>[] ?? userInterestedColumns.ToArray();
            var dropContracts = interestedColumns
                .Select(c => $"IF EXISTS (SELECT * FROM sys.service_message_types WHERE name = N'{databaseObjectsNaming}/{ChangeType.Delete}/{c.Item1}') DROP MESSAGE TYPE[{databaseObjectsNaming}/{ChangeType.Delete}/{c.Item1}];" + Environment.NewLine +
                             $"IF EXISTS (SELECT * FROM sys.service_message_types WHERE name = N'{databaseObjectsNaming}/{ChangeType.Insert}/{c.Item1}') DROP MESSAGE TYPE[{databaseObjectsNaming}/{ChangeType.Insert}/{c.Item1}];" + Environment.NewLine +
                             $"IF EXISTS (SELECT * FROM sys.service_message_types WHERE name = N'{databaseObjectsNaming}/{ChangeType.Update}/{c.Item1}') DROP MESSAGE TYPE[{databaseObjectsNaming}/{ChangeType.Update}/{c.Item1}];" + Environment.NewLine)
                .Concat(dropMessageStartEnd)
                .ToList();

            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandText = string.Format(Scripts.ScriptDropAll, databaseObjectsNaming, string.Join(Environment.NewLine, dropContracts));
                    sqlCommand.ExecuteNonQuery();
                }
            }

            Debug.WriteLine("SqlTableDependency: Database objects destroyed.");
        }

        private static void PreliminaryChecks(string connectionString, string tableName, string serviceBrokerName)
        {
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

                if (string.IsNullOrWhiteSpace(tableName))
                {
                    CheckIfServiceBrokerExists(sqlConnection, serviceBrokerName);
                }
                else
                {
                    CheckIfTableExists(sqlConnection, tableName);
                }
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

        private static void CheckIfServiceBrokerExists(SqlConnection sqlConnection, string serviceBrokerName)
        {
            using (var sqlCommand = sqlConnection.CreateCommand())
            {
                sqlCommand.CommandText = $"SELECT COUNT(*) FROM sys.services WHERE name = N'{serviceBrokerName}'";
                if ((int)sqlCommand.ExecuteScalar() == 0) throw new ServiceBrokerNotExistingException(serviceBrokerName);
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

        private IEnumerable<Tuple<string, SqlDbType, string>> GetUserInterestedColumns(IEnumerable<Tuple<string, SqlDbType, string>> tableColumnsList)
        {
            var modelPropertyInfos = ModelUtil.GetModelPropertiesInfo<T>();

            return (from modelPropertyInfo in modelPropertyInfos let propertyMappedTo = this._mapper?.GetMapping(modelPropertyInfo) select propertyMappedTo ?? modelPropertyInfo.Name into propertyName select tableColumnsList.FirstOrDefault(column => string.Equals(column.Item1.ToLower(), propertyName.ToLower(), StringComparison.CurrentCultureIgnoreCase)) into tableColumn where tableColumn != null select tableColumn).ToList();
        }

        private static void CheckUpdateOfValidity(IEnumerable<Tuple<string, SqlDbType, string>> tableColumnsList, IEnumerable<string> updateOf)
        {
            if (updateOf != null)
            {
                var columnsToMonitorDuringUpdate = updateOf as string[] ?? updateOf.ToArray();
                if (!columnsToMonitorDuringUpdate.Any()) throw new UpdateOfException("updateOf parameter is empty.");

                if (columnsToMonitorDuringUpdate.Any(string.IsNullOrWhiteSpace))
                {
                    throw new UpdateOfException("updateOf parameter contains a null or empty value.");
                }

                var tableColumns = tableColumnsList as Tuple<string, SqlDbType, string>[] ?? tableColumnsList.ToArray();
                var dbColumnNames = tableColumns.Select(t => t.Item1.ToLower()).ToList();
                foreach (var columnToMonitorDuringUpdate in columnsToMonitorDuringUpdate.Where(columnToMonitor => !dbColumnNames.Contains(columnToMonitor.ToLower())))
                {
                    throw new UpdateOfException($"Column '{columnToMonitorDuringUpdate}' specified on updateOf list does not exists");
                }
            }
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