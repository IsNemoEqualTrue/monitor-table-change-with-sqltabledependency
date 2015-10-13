////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Oracle.DataAccess.Client;
using TableDependency.Delegates;
using TableDependency.Enums;
using TableDependency.Exceptions;
using TableDependency.Extensions;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.Messages;
using TableDependency.OracleClient.EventArgs;
using TableDependency.OracleClient.Resources;
using TableDependency.Utilities;
using OracleConnection = Oracle.DataAccess.Client.OracleConnection;
using OracleConnectionStringBuilder = Oracle.DataAccess.Client.OracleConnectionStringBuilder;
using OracleException = Oracle.DataAccess.Client.OracleException;
using OracleParameter = Oracle.DataAccess.Client.OracleParameter;

namespace TableDependency.OracleClient
{
    /// <summary>
    /// OracleTableDependency class.
    /// </summary>
    public class OracleTableDependency<T> : ITableDependency<T>, IDisposable where T : class
    {
        #region Private variables

        private const string EndMessageTemplate = "{0}/EndDialog";
        private const string StartMessageTemplate = "{0}/StartDialog";

        private Task _task;
        private CancellationTokenSource _cancellationTokenSource;
        private readonly string _dataBaseObjectsNamingConvention;
        private readonly ModelToTableMapper<T> _mapper;
        private readonly string _connectionString;
        private readonly string _tableName;
        private readonly IEnumerable<Tuple<string, string, string>> _userInterestedColumns;
        private readonly IEnumerable<string> _updateOf;
        private bool _disposed;

        #endregion

        #region Properties

        /// <summary>
        /// Return the database objects naming convention. 
        /// </summary>
        /// <value>
        /// The data base objects naming.
        /// </value>
        public string DataBaseObjectsNamingConvention => string.Copy(this._dataBaseObjectsNamingConvention);

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
        /// Initializes a new instance of the <see cref="OracleTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="mapper">Model to columns table mapper.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        public OracleTableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper = null, IEnumerable<string> updateOf = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException(nameof(connectionString));
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException(nameof(tableName));

            PreliminaryChecks(connectionString, tableName);

            _dataBaseObjectsNamingConvention = Get24DigitsGuid();
            _connectionString = connectionString;
            _tableName = tableName;
            _mapper = mapper;            
            _updateOf = updateOf;
            _userInterestedColumns = GetColumnsToUseForCreatingDbObjects(updateOf);       

            Status = TableDependencyStatus.WaitingForStart;
        }

        #endregion

        #region Public methods

        /// <summary>
        /// Starts monitoring change in content's table.
        /// </summary>
        /// <param name="timeOut">The WAIT FOR timeout in seconds.</param>
        /// <param name="watchDogTimeOut">The watchDog timeout in seconds.</param>
        /// <exception cref="NoSubscriberException"></exception>
        /// <exception cref="TableDependency.Exceptions.NoSubscriberException"></exception>
        public void Start(int timeOut = 120, int watchDogTimeOut = 180)
        {
            if (timeOut < 60) throw new ArgumentException("timeOut must be greater or equal to 60 seconds");
            if (watchDogTimeOut < 60 || watchDogTimeOut < (timeOut + 60)) throw new ArgumentException("watchDogTimeOut must be at least 60 seconds bigger then timeOut");

            if (_task != null)
            {
                Debug.WriteLine("OracleTableDependency: Already called Start() method.");
                return;
            }

            if (OnChanged == null) throw new NoSubscriberException();
            var onChangedSubscribedList = OnChanged.GetInvocationList();
            var onErrorSubscribedList = OnError?.GetInvocationList();

            var processableMessages = CreateDatabaseObjects(_connectionString, _tableName, _userInterestedColumns, _updateOf, _dataBaseObjectsNamingConvention, timeOut, watchDogTimeOut);

            _cancellationTokenSource = new CancellationTokenSource();
            _task = Task.Factory.StartNew(() => 
                WaitForNotification(
                    _cancellationTokenSource.Token, 
                    onChangedSubscribedList,
                    onErrorSubscribedList,
                    OnStatusChanged,
                    _connectionString,
                    _dataBaseObjectsNamingConvention,
                    watchDogTimeOut,
                    _mapper,
                    processableMessages),
                _cancellationTokenSource.Token);

            Status = TableDependencyStatus.Starting;
            Debug.WriteLine("OracleTableDependency: Started waiting for notification.");
        }

        public void Stop()
        {
            if (_task != null)
            {
                _cancellationTokenSource.Cancel(true);
                _task?.Wait();
            }

            _task = null;

            DropDatabaseObjects(_connectionString, _dataBaseObjectsNamingConvention);

            _disposed = true;

            Debug.WriteLine("OracleTableDependency: Stopped waiting for notification.");
        }

#if DEBUG
        public void StopMantainingDatabaseObjects()
        {
            if (_task != null)
            {
                _cancellationTokenSource.Cancel(true);
                _task?.Wait();
            }

            _task = null;

            _disposed = true;

            Debug.WriteLine("OracleTableDependency: Stopped waiting for notification.");
        }
#endif

        #endregion

        #region Private methods

        private async static Task WaitForNotification(
            CancellationToken cancellationToken,
            Delegate[] onChangeSubscribedList,
            Delegate[] onErrorSubscribedList,
            Action<TableDependencyStatus> setStatus,
            string connectionString,
            string databaseObjectsNaming,
            int timeOutWatchDog,
            ModelToTableMapper<T> modelMapper,
            ICollection<string> processableMessages)
        {            
            setStatus(TableDependencyStatus.Started);

            var messagesBag = new MessagesBag(string.Format(StartMessageTemplate, databaseObjectsNaming), string.Format(EndMessageTemplate, databaseObjectsNaming));

            try
            {
                while (true)
                {
                    StartWatchDog(connectionString, databaseObjectsNaming, timeOutWatchDog);

                    using (var transactionScope = new TransactionScope(TransactionScopeOption.RequiresNew, TimeSpan.MaxValue, TransactionScopeAsyncFlowOption.Enabled))
                    {
                        using (var connection = new OracleConnection(connectionString))
                        {
                            await connection.OpenAsync(cancellationToken);
                                                                                 
                            using (var getQueueMessageCommand = connection.CreateCommand())
                            {
                                getQueueMessageCommand.CommandText = $"DEQ_{databaseObjectsNaming}";
                                getQueueMessageCommand.CommandType = CommandType.StoredProcedure;
                                getQueueMessageCommand.CommandTimeout = 0;
                                getQueueMessageCommand.Parameters.Add(new OracleParameter {ParameterName = "p_recordset", OracleDbType = OracleDbType.RefCursor, Direction = ParameterDirection.Output});

                                try
                                {
                                    setStatus(TableDependencyStatus.WaitingForNotification);

                                    using (var reader = await getQueueMessageCommand.ExecuteReaderAsync(cancellationToken))
                                    {
                                        while (await reader.ReadAsync(cancellationToken))
                                        {
                                            var messageType = reader.GetString(0);
                                            if (processableMessages.Contains(messageType))
                                            {
                                                var messageContent = reader.IsDBNull(1) ? null : reader.GetString(1);

                                                var messageStatus = messagesBag.AddMessage(messageType, GetBytes(messageContent));
                                                if (messageStatus == MessagesBagStatus.Closed)
                                                {
                                                    RaiseEvent(onChangeSubscribedList, modelMapper, messagesBag);
                                                    transactionScope.Complete();
                                                    break;
                                                }
                                            }
                                        }
                                    }                                    
                                }
                                catch (Exception exception)
                                {
                                    ThrowIfOracleClientCancellationRequested(cancellationToken, exception);
                                    throw;
                                }
                            }
                        }
                    }

                    StopWatchDog(connectionString, databaseObjectsNaming);
                }
            }
            catch (OperationCanceledException)
            {
                setStatus(TableDependencyStatus.StoppedDueToCancellation);
                Debug.WriteLine("OracleTableDependency: Operation canceled.");
            }
            catch (Exception exception)
            {
                setStatus(TableDependencyStatus.StoppedDueToError);
                Debug.WriteLine("OracleTableDependency: Exception " + exception.Message + ".");
                if (cancellationToken.IsCancellationRequested == false) NotifyListenersAboutError(onErrorSubscribedList, exception);
            }
        }

        private static void StartWatchDog(string connectionString, string databaseObjectsNaming, int timeOutWatchDog)
        {
            using (var connection = new OracleConnection(connectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = string.Format(Scripts.ScriptJobEnable, databaseObjectsNaming, (timeOutWatchDog / 60));
                    command.CommandType = CommandType.Text;
                    command.ExecuteNonQuery();
                }
            }
        }

        private static void StopWatchDog(string connectionString, string databaseObjectsNaming)
        {
            using (var connection = new OracleConnection(connectionString))
            {
                connection.Open();
                using (var watchDogDisableCommand = connection.CreateCommand())
                {
                    watchDogDisableCommand.CommandText = string.Format(Scripts.ScriptJobDisable, databaseObjectsNaming);
                    watchDogDisableCommand.CommandType = CommandType.Text;
                    watchDogDisableCommand.ExecuteNonQuery();
                }
            }
        }

        private void OnStatusChanged(TableDependencyStatus status)
        {
            Status = status;
        }

        private static byte[] GetBytes(string str, int? lenght = null)
        {
            if (str == null) return null;

            byte[] bytes = lenght.HasValue ? new byte[lenght.Value] : new byte[str.Length * sizeof(char)];
            Buffer.BlockCopy(str.ToCharArray(), 0, bytes, 0, str.Length * sizeof(char));
            return bytes;
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

        private static void ThrowIfOracleClientCancellationRequested(CancellationToken cancellationToken, Exception exception)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                var oracleException = exception as OracleException;
                if (null == oracleException)
                {
                    var aggregateException = exception as AggregateException;
                    if (aggregateException != null) oracleException = aggregateException.InnerException as OracleException;
                    if (oracleException == null) return;
                }

                if (oracleException.Number != 1013) return;

                throw new OperationCanceledException();
            }
        }

        private static void RaiseEvent(IEnumerable<Delegate> delegates, ModelToTableMapper<T> modelMapper, MessagesBag messagesBag)
        {
            if (delegates == null) return;
            foreach (var dlg in delegates.Where(d => d != null)) dlg.Method.Invoke(dlg.Target, new object[] { null, new OracleRecordChangedEventArgs<T>(messagesBag, modelMapper) });
        }

        private static string ComputeVariableSize(Tuple<string, string, string> column)
        {
            if (string.IsNullOrWhiteSpace(column.Item3)) return string.Empty;
            if (Convert.ToInt32(column.Item3) > 0) return "(" + column.Item3 + ")";
            return string.Empty;
        }

        private static IList<string> CreateDatabaseObjects(string connectionString, string tableName, IEnumerable<Tuple<string, string, string>> columnsTableList, IEnumerable<string> updateOf, string databaseObjectsNaming, int timeOut, int timeOutWatchDog)
        {
            try
            {
                using (var connection = new OracleConnection(connectionString))
                {
                    connection.Open();

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = $"CREATE TYPE TYPE_{databaseObjectsNaming} AS OBJECT(MESSAGE_TYPE VARCHAR2(50), MESSAGE_CONTENT VARCHAR2(4000));";                       
                        command.ExecuteNonQuery();
                        command.CommandText = $"CREATE TYPE TBL_{databaseObjectsNaming} IS TABLE OF TYPE_{databaseObjectsNaming};";
                        command.ExecuteNonQuery();

                        command.CommandText = $"BEGIN DBMS_AQADM.CREATE_QUEUE_TABLE(queue_table=> 'QT_{databaseObjectsNaming}', queue_payload_type=> 'TYPE_{databaseObjectsNaming}', multiple_consumers => FALSE); END;";
                        command.ExecuteNonQuery();
                        command.CommandText = $"BEGIN DBMS_AQADM.CREATE_QUEUE(queue_name => 'QUE_{databaseObjectsNaming}', queue_table => 'QT_{databaseObjectsNaming}'); END;";
                        command.ExecuteNonQuery();
                        command.CommandText = $"BEGIN DBMS_AQADM.START_QUEUE(queue_name=> 'QUE_{databaseObjectsNaming}'); END;";
                        command.ExecuteNonQuery();

                        var declareStatement = string.Join(Environment.NewLine, columnsTableList.Select(c => "v_" + c.Item1.Replace(" ", "_").Replace("\"", string.Empty) + " " + c.Item2 + ComputeVariableSize(c) + ";"));
                        var startMessageStatement = string.Format(StartMessageTemplate, databaseObjectsNaming);
                        var endMessageStatement = string.Format(EndMessageTemplate, databaseObjectsNaming);
                        var setNewValueStatement = string.Join(Environment.NewLine, columnsTableList.Select(c => "v_" + c.Item1.Replace(" ", "_").Replace("\"", string.Empty) + " := :NEW." + c.Item1 + ";"));
                        var setOldValueStatement = string.Join(Environment.NewLine, columnsTableList.Select(c => "v_" + c.Item1.Replace(" ", "_").Replace("\"", string.Empty) + " := :OLD." + c.Item1 + ";"));
                        var insertDml = ChangeType.Insert.ToString();
                        var updateDml = ChangeType.Update.ToString();
                        var deleteDml = ChangeType.Delete.ToString();

                        var enqueueStatement = string.Join(Environment.NewLine, columnsTableList.Select(c =>
                        {
                            var messageType = $"'{databaseObjectsNaming}/' || dmlType || '/{c.Item1.Replace("\"", string.Empty)}'";
                            var variableName = "TO_CHAR(v_" + c.Item1.Replace(" ", "_").Replace("\"", string.Empty) + ")";
                            return string.Format(Scripts.EnqueueScript, databaseObjectsNaming, databaseObjectsNaming, messageType, variableName);
                        }));

                        command.CommandText = string.Format(
                            Scripts.CreateTriggerEnqueueMessage,
                            databaseObjectsNaming,
                            GetUpdateOfStatement(updateOf),
                            tableName,
                            startMessageStatement,
                            endMessageStatement,
                            declareStatement,                            
                            insertDml,
                            setNewValueStatement,
                            updateDml,
                            setNewValueStatement,
                            deleteDml,
                            setOldValueStatement,
                            enqueueStatement);
                        command.ExecuteNonQuery();

                        command.CommandText = string.Format(Scripts.CreateProcedureDequeueMessage, databaseObjectsNaming, timeOut, columnsTableList.Count() + 2);
                        command.ExecuteNonQuery();

                        command.CommandText = string.Format(Scripts.ScriptJobCreate, databaseObjectsNaming, (timeOutWatchDog / 60), string.Format(Scripts.ScriptDropAll, databaseObjectsNaming).Replace("'", "''"));
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch
            {
                DropDatabaseObjects(connectionString, databaseObjectsNaming);
                throw;
            }

            Debug.WriteLine($"OracleTableDependency: Database objects created with naming {databaseObjectsNaming}.");

            var insertMessageTypes = columnsTableList.Select(c => $"{databaseObjectsNaming}/{ChangeType.Insert}/{c.Item1.Replace("\"", string.Empty)}").ToList();
            var updateMessageTypes = columnsTableList.Select(c => $"{databaseObjectsNaming}/{ChangeType.Update}/{c.Item1.Replace("\"", string.Empty)}").ToList();
            var deleteMessageTypes = columnsTableList.Select(c => $"{databaseObjectsNaming}/{ChangeType.Delete}/{c.Item1.Replace("\"", string.Empty)}").ToList();
            var messageBoundaries = new List<string> { string.Format(StartMessageTemplate, databaseObjectsNaming), string.Format(EndMessageTemplate, databaseObjectsNaming) };        

            return insertMessageTypes.Concat(updateMessageTypes).Concat(deleteMessageTypes).Concat(messageBoundaries).ToList();
        }

        private static void DropDatabaseObjects(string connectionString, string databaseObjectsNaming)
        {
            using (var connection = new OracleConnection(connectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = string.Format("DECLARE counter INT; BEGIN select COUNT(*) INTO counter FROM user_scheduler_jobs WHERE JOB_NAME = 'JOB_{0}'; DBMS_SCHEDULER.DROP_JOB('JOB_{0}', TRUE); EXCEPTION WHEN NO_DATA_FOUND THEN NULL; END;", databaseObjectsNaming);
                    command.ExecuteNonQuery();

                    command.CommandType = CommandType.Text;
                    command.CommandText = string.Format(Scripts.ScriptDropAll, databaseObjectsNaming);
                    command.ExecuteNonQuery();
                }
            }

            Debug.WriteLine("OracleTableDependency: Database objects destroyed.");
        }

        private static void PreliminaryChecks(string connectionString, string tableName)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException(nameof(connectionString));
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException(nameof(tableName));

            CheckIfConnectionStringIsValid(connectionString);

            using (var connection = new OracleConnection(connectionString))
            {
                try
                {
                    connection.Open();
                }
                catch (OracleException exception)
                {
                    throw new InvalidConnectionStringException(exception);
                }

                CheckIfTableExists(connection, tableName);
            }
        }

        private static void CheckIfConnectionStringIsValid(string connectionString)
        {
            try
            {
                new OracleConnectionStringBuilder(connectionString);
            }
            catch (Exception exception)
            {
                throw new InvalidConnectionStringException(exception);
            }
        }

        private static void CheckIfTableExists(OracleConnection connection, string tableName)
        {
            using (var command = connection.CreateCommand())
            {
                var countParam = new OracleParameter { ParameterName = "exist", OracleDbType = OracleDbType.Int32, Direction = ParameterDirection.Output };
                command.CommandText = $"BEGIN SELECT COUNT(*) INTO :exist FROM SYS.USER_TABLES WHERE UPPER(TABLE_NAME) = '{tableName.ToUpper()}'; END;";
                command.Parameters.Add(countParam);
                command.ExecuteNonQuery();

                if (int.Parse(countParam.Value.ToString()) == 0) throw new NotExistingTableException(tableName);
            }
        }

        private static string Get24DigitsGuid()
        {
            return Guid.NewGuid().ToString().Substring(5, 20).Replace("-", "_").ToUpper();
        }     

        private static string GetUpdateOfStatement(IEnumerable<string> columnsUpdateOf)
        {
            return columnsUpdateOf != null
                ? " OF " + string.Join(", ", columnsUpdateOf.Where(c => !string.IsNullOrWhiteSpace(c)).Distinct(StringComparer.CurrentCultureIgnoreCase).Select(c => $"\"{c}\"").ToList())
                : null;
        }

        private IEnumerable<Tuple<string, string, string>> GetColumnsToUseForCreatingDbObjects(IEnumerable<string> updateOf)
        {
            var tableColumns = GetTableColumnsList(_connectionString, _tableName);
            var tableColumnsList = tableColumns as Tuple<string, string, string>[] ?? tableColumns.ToArray();
            if (!tableColumnsList.Any()) throw new NoColumnsException(_tableName);

            CheckUpdateOfValidity(tableColumnsList, updateOf);
            CheckMapperValidity(tableColumnsList);

            var userIterestedColumns = GetUserInterestedColumns(tableColumnsList);

            var columnsToUseForCreatingDbObjects = userIterestedColumns as Tuple<string, string, string>[] ?? userIterestedColumns.ToArray();
            CheckIfUserInterestedColumnsCanBeManaged(columnsToUseForCreatingDbObjects);
            return columnsToUseForCreatingDbObjects;
        }

        private void CheckIfUserInterestedColumnsCanBeManaged(IEnumerable<Tuple<string, string, string>> tableColumnsToUse)
        {
            foreach (var tableColumn in tableColumnsToUse)
            {
                switch (tableColumn.Item2.ToUpper())
                {
                    case "BFILE":
                    case "BLOB":
                    case "CLOB":
                    case "NLOB":
                        throw new ColumnTypeNotSupportedException($"{tableColumn.Item2} type is not an admitted for SqlTableDependency.");
                }
            }
        }

        private IEnumerable<Tuple<string, string, string>> GetUserInterestedColumns(IEnumerable<Tuple<string, string, string>> tableColumnsList)
        {
            var tableColumnsListFiltered = new List<Tuple<string, string, string>>();

            foreach (var entityPropertyInfo in ModelUtil.GetModelPropertiesInfo<T>())
            {
                var propertyMappedTo = _mapper?.GetMapping(entityPropertyInfo);
                var propertyName = propertyMappedTo ?? entityPropertyInfo.Name;

                // If model property is mapped to table column keep it
                foreach (var tableColumn in tableColumnsList)
                {
                    if (string.Equals(tableColumn.Item1.ToLower(), "\"" + propertyName.ToLower() + "\"", StringComparison.CurrentCultureIgnoreCase))
                    {
                        tableColumnsListFiltered.Add(tableColumn);
                        break;
                    }
                }
            }

            return tableColumnsListFiltered;
        }

        private static void CheckUpdateOfValidity(IEnumerable<Tuple<string, string, string>> tableColumnsList, IEnumerable<string> updateOf)
        {
            if (updateOf != null)
            {
                var columnsToMonitorDuringUpdate = updateOf as string[] ?? updateOf.ToArray();
                if (!columnsToMonitorDuringUpdate.Any()) throw new UpdateOfException("updateOf parameter is empty.");

                if (columnsToMonitorDuringUpdate.Any(string.IsNullOrWhiteSpace))
                {
                    throw new UpdateOfException("updateOf parameter contains a null or empty value.");
                }

                var tableColumns = tableColumnsList as Tuple<string, string, string>[] ?? tableColumnsList.ToArray();
                var dbColumnNames = tableColumns.Select(t => t.Item1.ToLower()).ToList();
                foreach (var columnToMonitorDuringUpdate in columnsToMonitorDuringUpdate.Where(columnToMonitor => !dbColumnNames.Contains("\"" + columnToMonitor.ToLower() + "\"")))
                {
                    throw new UpdateOfException($"updateOf define column {columnToMonitorDuringUpdate} that does not exists");
                }
            }
        }

        private void CheckMapperValidity(IEnumerable<Tuple<string, string, string>> tableColumnsList)
        {
            if (_mapper != null)
            {
                if (_mapper.Count() < 1) throw new ModelToTableMapperException();

                // With ORACLE when define an column with "" it become case sensitive.
                var dbColumnNames = tableColumnsList.Select(t => t.Item1).ToList();
                var mappingNames = _mapper.GetMappings().Select(t => "\"" + t.Value + "\"").ToList();

                mappingNames.ForEach<string>(mapping =>
                {
                    if (dbColumnNames.Contains(mapping) == false) throw new ModelToTableMapperException();
                });
            }
        }

        private static IEnumerable<Tuple<string, string, string>> GetTableColumnsList(string connectionString, string tableName)
        {
            var columnsList = new List<Tuple<string, string, string>>();

            using (var connection = new OracleConnection(connectionString))
            {
                connection.Open();
                using (var cmmand = connection.CreateCommand())
                {
                    cmmand.CommandText = $"SELECT COLUMN_NAME, DATA_TYPE, TO_CHAR(NVL(CHAR_LENGTH, 0)) AS CHAR_LENGTH FROM SYS.USER_TAB_COLUMNS WHERE UPPER(TABLE_NAME) = '{tableName.ToUpper()}' ORDER BY COLUMN_ID";
                    var reader = cmmand.ExecuteReader();
                    while (reader.Read())
                    {
                        var name = reader.GetString(0);
                        var type = reader.GetString(1);
                        var size = reader.GetString(2);
                        columnsList.Add(new Tuple<string, string, string>("\"" + name + "\"", type, size));
                    }
                }
            }

            return columnsList;
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

        ~OracleTableDependency()
        {
            Dispose(false);
        }

        #endregion
    }
}