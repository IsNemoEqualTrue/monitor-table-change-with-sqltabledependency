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
using Oracle.DataAccess.Client;
using Oracle.DataAccess.Types;
using TableDependency.Delegates;
using TableDependency.Enums;
using TableDependency.Exceptions;
using TableDependency.Extensions;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.OracleClient.EventArgs;
using TableDependency.OracleClient.Exceptions;
using TableDependency.OracleClient.MessageTypes;
using TableDependency.OracleClient.Resources;
using OracleCommand = Oracle.DataAccess.Client.OracleCommand;
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

        private Task _task;
        private CancellationTokenSource _cancellationTokenSource;
        private readonly string _dataBaseObjectsNamingConvention;
        private readonly ModelToTableMapper<T> _modelToTableMapper;
        private readonly string _updateOf;
        private readonly string _connectionString;
        private readonly string _tableName;
        private readonly string _messagePayloadForInsertAndUpdate;
        private readonly string _messagePayloadForDelete;
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
        /// <param name="mapper">The model to columns table mapper.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        public OracleTableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper = null, IEnumerable<string> updateOf = null)
        {
            PreliminaryChecks(connectionString, tableName);

            _modelToTableMapper = mapper;
            _connectionString = connectionString;
            _tableName = tableName;
            _dataBaseObjectsNamingConvention = Get24DigitsGuid();

            var columnsToUseCreatingTrigger = GetColumnsToUseForCreationDbObjects(updateOf);
            var toUseCreatingTrigger = columnsToUseCreatingTrigger as Tuple<string, string, string>[] ?? columnsToUseCreatingTrigger.ToArray();

            _messagePayloadForInsertAndUpdate = PrepareTSqlTriggerPartialBody(tableName, toUseCreatingTrigger, "NEW");
            _messagePayloadForDelete = PrepareTSqlTriggerPartialBody(tableName, toUseCreatingTrigger, "OLD");
            _updateOf = GetUpdateOf(updateOf);

            this.Status = TableDependencyStatus.WaitingToStart;
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
            if (timeOut < 60) throw new ArgumentException("timeOut must be great or equal to 60 seconds");
            if (watchDogTimeOut < 60 || watchDogTimeOut < (timeOut + 60)) throw new ArgumentException("watchDogTimeOut must be at least 60 seconds bigger then timeOut");

            if (_task != null)
            {
                Debug.WriteLine("OracleTableDependency: Already called Start() method.");
                return;
            }

            if (OnChanged == null) throw new NoSubscriberException();
            var onChangedSubscribedList = OnChanged.GetInvocationList();
            var onErrorSubscribedList = OnError?.GetInvocationList();

            this.Status = TableDependencyStatus.Starting;

            CreateDatabaseObjects(_connectionString, _tableName, this._messagePayloadForInsertAndUpdate, _messagePayloadForDelete, _updateOf, _dataBaseObjectsNamingConvention, timeOut, watchDogTimeOut);

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
                    _modelToTableMapper),
                _cancellationTokenSource.Token);

            Debug.WriteLine("OracleTableDependency: Started waiting for notification.");
        }

        /// <summary>
        /// Stops monitoring change in the table contents.
        /// </summary>
        public void Stop()
        {
            if (_task != null)
            {
                _cancellationTokenSource.Cancel(true);
                if (_task.Status == TaskStatus.Running) _task.Wait();
            }

            _task = null;

            DropDatabaseObjects(_connectionString, this._dataBaseObjectsNamingConvention);

            _disposed = true;

            Debug.WriteLine("OracleTableDependency: Stopped waiting for notification.");
        }

#if DEBUG
        public void StopMantainingDatabaseObjects()
        {
            if (_task != null)
            {
                _cancellationTokenSource.Cancel(true);
                if (_task.Status == TaskStatus.Running) _task.Wait();
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
            ModelToTableMapper<T> modelMapper)
        {
            OracleCommand watchDogEnableCommand = null;
            OracleCommand watchDogDisableCommand = null;
            OracleCommand getQueueMessageCommand = null;

            setStatus(TableDependencyStatus.Started);

            try
            {
                using (var connection = new OracleConnection(connectionString))
                {
                    connection.Open();

                    OracleParameter messageTypeParameter;
                    OracleParameter messageContentParameter;

                    watchDogEnableCommand = PrepareWatchDogEnableCommand(databaseObjectsNaming, timeOutWatchDog, connection);
                    watchDogDisableCommand = PrepareWatchDogDisableCommand(databaseObjectsNaming, connection);
                    getQueueMessageCommand = PrepareGetQueueMessageCommand(databaseObjectsNaming, connection, out messageTypeParameter, out messageContentParameter);

                    while (true)
                    {
                        try
                        {
                            setStatus(TableDependencyStatus.ListenerForNotification);

                            await watchDogEnableCommand.ExecuteNonQueryAsync(cancellationToken).WithCancellation(cancellationToken);
                            await getQueueMessageCommand.ExecuteNonQueryAsync(cancellationToken).WithCancellation(cancellationToken);
                            await watchDogDisableCommand.ExecuteNonQueryAsync(cancellationToken).WithCancellation(cancellationToken);

                            var messageTypeValue = Convert.ToString(messageTypeParameter.Value);
                            if (messageTypeValue != CustomMessageTypes.TimeoutMessageType)
                            {
                                if (!string.IsNullOrWhiteSpace(messageTypeValue)) RaiseEvent(onChangeSubscribedList, messageTypeValue, (OracleXmlType)messageContentParameter.Value, modelMapper);
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
            finally
            {
                watchDogEnableCommand?.Dispose();
                watchDogDisableCommand?.Dispose();
                getQueueMessageCommand?.Dispose();
            }
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

        private static OracleCommand PrepareWatchDogEnableCommand(string databaseObjectsNaming, int timeOutWatchDog, OracleConnection connection)
        {
            var watchDogEnableCommand = connection.CreateCommand();
            watchDogEnableCommand.CommandText = string.Format(Scripts.ScriptJobEnable, databaseObjectsNaming, (timeOutWatchDog / 60));
            watchDogEnableCommand.CommandType = CommandType.Text;
            watchDogEnableCommand.Prepare();

            return watchDogEnableCommand;
        }

        private static OracleCommand PrepareWatchDogDisableCommand(string databaseObjectsNaming, OracleConnection connection)
        {
            var watchDogDisableCommand = connection.CreateCommand();
            watchDogDisableCommand.CommandText = string.Format(Scripts.ScriptJobDisable, databaseObjectsNaming);
            watchDogDisableCommand.CommandType = CommandType.Text;
            watchDogDisableCommand.Prepare();

            return watchDogDisableCommand;
        }

        private static OracleCommand PrepareGetQueueMessageCommand(string databaseObjectsNaming, OracleConnection connection, out OracleParameter messageTypeParameter, out OracleParameter messageContentParameter)
        {
            var getQueueMessageCommand = connection.CreateCommand();
            getQueueMessageCommand.CommandText = $"DEQ_{databaseObjectsNaming}";
            getQueueMessageCommand.CommandType = CommandType.StoredProcedure;
            getQueueMessageCommand.CommandTimeout = 0;
            messageTypeParameter = getQueueMessageCommand.Parameters.Add(new OracleParameter { ParameterName = "messageType", OracleDbType = OracleDbType.Varchar2, Size = 50, Direction = ParameterDirection.Output });
            messageContentParameter = getQueueMessageCommand.Parameters.Add(new OracleParameter { ParameterName = "messageContent ", OracleDbType = OracleDbType.XmlType, Direction = ParameterDirection.Output });
            getQueueMessageCommand.Prepare();

            return getQueueMessageCommand;
        }

        private static void RaiseEvent(IEnumerable<Delegate> delegates, string messageType, OracleXmlType message, ModelToTableMapper<T> modelMapper)
        {
            if (delegates == null) return;
            if (string.IsNullOrWhiteSpace(messageType)) return;
            if (string.IsNullOrWhiteSpace(message.Value)) return;

            foreach (var dlg in delegates.Where(d => d != null))
            {
                dlg.Method.Invoke(dlg.Target, new object[] { null, new OracleRecordChangedEventArgs<T>(messageType, message.Value, modelMapper) });
            }
        }

        private static void CreateDatabaseObjects(string connectionString, string tableName, string messagePayloadForInsertUpdate, string messagePayloadForDelete, string updateOf, string databaseObjectsNaming, int timeOut, int timeOutWatchDog)
        {
            try
            {
                using (var connection = new OracleConnection(connectionString))
                {
                    connection.Open();

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = string.Format(
                            Scripts.CreateTypeMessage,
                            databaseObjectsNaming);
                        command.ExecuteNonQuery();

                        command.CommandText = string.Format(
                            Scripts.CreateQueue,
                            databaseObjectsNaming);
                        command.ExecuteNonQuery();

                        command.CommandText = string.Format(
                            Scripts.CreateTriggerEnqueueMessage,
                            databaseObjectsNaming,
                            tableName,
                            CustomMessageTypes.InsertedMessageType,
                            CustomMessageTypes.UpdatedMessageType,
                            CustomMessageTypes.DeletedMessageType,
                            messagePayloadForInsertUpdate,
                            messagePayloadForDelete,
                            updateOf);
                        command.ExecuteNonQuery();

                        command.CommandText = string.Format(
                            Scripts.CreateProcedureDequeueMessage,
                            databaseObjectsNaming,
                            timeOut,
                            CustomMessageTypes.TimeoutMessageType);
                        command.ExecuteNonQuery();

                        command.CommandText = string.Format(
                            Scripts.ScriptJobCreate,
                            databaseObjectsNaming,
                            (timeOutWatchDog / 60),
                            string.Format(Scripts.ScriptDropAll, databaseObjectsNaming).Replace("'", "''"));
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
                var countParam = new OracleParameter { ParameterName = ":1", OracleDbType = OracleDbType.Int32, Direction = ParameterDirection.Output };
                command.CommandText = $"BEGIN SELECT COUNT(*) INTO :1 FROM SYS.USER_TABLES WHERE TABLE_NAME = '{tableName}'; END;";
                command.Parameters.Add(countParam);
                command.ExecuteNonQuery();

                if (int.Parse(countParam.Value.ToString()) == 0) throw new NotExistingTableException(tableName);
            }
        }

        private static string Get24DigitsGuid()
        {
            return Guid.NewGuid().ToString().Substring(5, 20).Replace("-", "_").ToUpper();
        }

        private string PrepareTSqlTriggerPartialBody(string tableName, IEnumerable<Tuple<string, string, string>> columnsTableList, string referencing)
        {
            var columnsList = columnsTableList.Select(c =>
            {
                var referencingPart = $" || :{referencing}.{c.Item1} || ";
                var columnNode = $"'<column name={c.Item1}><![CDATA['{referencingPart}']]></column>'";
                return columnNode;
            })
            .ToList();

            var columns = string.Join(" || ", columnsList);

            return string.Format("'<?xml version=\"1.0\"?><{0}>' || {1} || '</{0}>'", tableName.ToLower(), columns);
        }

        private string GetUpdateOf(IEnumerable<string> columnsUpdateOf)
        {
            return columnsUpdateOf != null
                ? " OF " + string.Join(", ", columnsUpdateOf.Where(c => !string.IsNullOrWhiteSpace(c)).Distinct(StringComparer.CurrentCultureIgnoreCase).Select(c => $"\"{c}\"").ToList())
                : null;
        }

        private IEnumerable<Tuple<string, string, string>> GetColumnsToUseForCreationDbObjects(IEnumerable<string> updateOf)
        {
            var tableColumnsList = GetTableColumnsList(_connectionString, _tableName);
            var columnsList = tableColumnsList as Tuple<string, string, string>[] ?? tableColumnsList.ToArray();
            if (!columnsList.Any()) throw new NoColumnsException(_tableName);
            CheckMapperValidity(columnsList);

            IEnumerable<Tuple<string, string, string>> columnsToUse = null;
            if (updateOf != null)
            {
                var insterestedColumnList = updateOf as string[] ?? updateOf.ToArray();
                CheckUpdateOfValidity(columnsList, insterestedColumnList);
                columnsToUse = FilterColumnBaseOnUpdateOf(columnsList, insterestedColumnList);
            }
            else
            {
                columnsToUse = tableColumnsList;
            }

            CheckColumnsManageability(columnsToUse);

            return columnsToUse;
        }

        private void CheckColumnsManageability(IEnumerable<Tuple<string, string, string>> tableColumnsToUse)
        {
            foreach (var tableColumn in tableColumnsToUse)
            {
                switch (tableColumn.Item2.ToUpper())
                {
                    case "BFILE":
                    case "BLOB":
                    case "CLOB":
                    case "RAW":
                        throw new ColumnTypeNotSupportedException(_tableName, tableColumn.Item2);
                }
            }
        }

        private static IEnumerable<Tuple<string, string, string>> FilterColumnBaseOnUpdateOf(IEnumerable<Tuple<string, string, string>> tableColumnsList, IEnumerable<string> updateOf)
        {
            if (updateOf != null && updateOf.Any())
            {
                var filteredList = new List<Tuple<string, string, string>>();

                foreach (var tableColumn in tableColumnsList)
                {
                    foreach (var interestedColumn in updateOf)
                    {
                        if (string.Equals(tableColumn.Item1.ToLower(), "\"" + interestedColumn.ToLower() + "\"", StringComparison.CurrentCultureIgnoreCase))
                        {
                            filteredList.Add(tableColumn);
                            break;
                        }
                    }
                }

                return filteredList;
            }

            return tableColumnsList;
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
                    throw new UpdateOfException($"Column {columnToMonitorDuringUpdate} does not exists");
                }
            }
        }

        private void CheckMapperValidity(IEnumerable<Tuple<string, string, string>> tableColumnsList)
        {
            if (_modelToTableMapper != null)
            {
                if (_modelToTableMapper.Count() < 1) throw new ModelToTableMapperException();

                // With ORACLE when define an column with "" it become case sensitive.
                var dbColumnNames = tableColumnsList.Select(t => t.Item1).ToList();
                var mappingNames = _modelToTableMapper.GetMappings().Select(t => "\"" + t.Value + "\"").ToList();

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
                    cmmand.CommandText = $"SELECT COLUMN_NAME, DATA_TYPE, TO_CHAR(NVL(CHAR_LENGTH, 0)) AS CHAR_LENGTH FROM SYS.USER_TAB_COLUMNS WHERE TABLE_NAME = '{tableName}' ORDER BY COLUMN_ID";
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