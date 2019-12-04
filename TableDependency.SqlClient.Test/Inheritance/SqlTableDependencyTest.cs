using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using TableDependency.SqlClient.Extensions;
using TableDependency.SqlClient.Base.Abstracts;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.Exceptions;
using TableDependency.SqlClient.Base.Messages;
using TableDependency.SqlClient.Base.Utilities;
using TableDependency.SqlClient.Exceptions;
using TableDependency.SqlClient.Messages;
using TableDependency.SqlClient.Resources;
using TableDependency.SqlClient.Base.Delegates;

namespace TableDependency.SqlClient.Test.Inheritance
{
    public class SqlTableDependencyTest<T> : SqlTableDependency<T> where T : class, new()
    {
        public override event ErrorEventHandler OnError;
        public override event ChangedEventHandler<T> OnChanged;
        public override event StatusEventHandler OnStatusChanged;

        private bool _stopWithoutDisposing;
        private bool _throwExceptionBeforeWaitForNotifications;
        private bool _throwExceptionInWaitForNotificationsPoint1;
        private bool _throwExceptionInWaitForNotificationsPoint2;
        private bool _throwExceptionInWaitForNotificationsPoint3;
        private bool _throwExceptionCreateSqlServerDatabaseObjects;

        public SqlTableDependencyTest(
            string connectionString,
            string tableName = null,
            string schemaName = null,
            IModelToTableMapper<T> mapper = null,
            IUpdateOfModel<T> updateOf = null,
            ITableDependencyFilter filter = null,
            DmlTriggerType notifyOn = DmlTriggerType.All,
            bool executeUserPermissionCheck = true,
            bool includeOldValues = false,
            bool stopWithoutDisposing = false,
            bool throwExceptionBeforeWaitForNotifications = false,
            bool throwExceptionInWaitForNotificationsPoint1 = false,
            bool throwExceptionInWaitForNotificationsPoint2 = false,
            bool throwExceptionInWaitForNotificationsPoint3 = false,
            bool throwExceptionCreateSqlServerDatabaseObjects = false) : base(connectionString, tableName, schemaName, mapper, updateOf, filter, notifyOn, executeUserPermissionCheck, includeOldValues)
        {
            _stopWithoutDisposing = stopWithoutDisposing;
            _throwExceptionBeforeWaitForNotifications = throwExceptionBeforeWaitForNotifications;
            _throwExceptionInWaitForNotificationsPoint1 = throwExceptionInWaitForNotificationsPoint1;
            _throwExceptionInWaitForNotificationsPoint2 = throwExceptionInWaitForNotificationsPoint2;
            _throwExceptionInWaitForNotificationsPoint3 = throwExceptionInWaitForNotificationsPoint3;
            _throwExceptionCreateSqlServerDatabaseObjects = throwExceptionCreateSqlServerDatabaseObjects;
        }

        public override void Start(int timeOut = 120, int watchDogTimeOut = 180)
        {
            if (this.OnChanged == null) throw new NoSubscriberException();

            var onChangedSubscribedList = this.OnChanged?.GetInvocationList();
            var onErrorSubscribedList = this.OnError?.GetInvocationList();
            var onStatusChangedSubscribedList = this.OnStatusChanged?.GetInvocationList();

            this.NotifyListenersAboutStatus(onStatusChangedSubscribedList, TableDependencyStatus.Starting);

            _cancellationTokenSource = new CancellationTokenSource();

            if (_throwExceptionBeforeWaitForNotifications) throw new Exception();

            _task = Task.Factory.StartNew(() =>
                WaitForNotifications(
                    _cancellationTokenSource.Token,
                    onChangedSubscribedList,
                    onErrorSubscribedList,
                    onStatusChangedSubscribedList,
                    timeOut,
                    watchDogTimeOut),
                _cancellationTokenSource.Token);

            this.WriteTraceMessage(TraceLevel.Info, $"Waiting for receiving {_tableName}'s records change notifications.");
        }

        protected override IList<string> CreateSqlServerDatabaseObjects(IEnumerable<TableColumnInfo> userInterestedColumns, string columnsForUpdateOf, int watchDogTimeOut)
        {
            var processableMessages = new List<string>();
            var tableColumns = userInterestedColumns as IList<TableColumnInfo> ?? userInterestedColumns.ToList();

            var columnsForModifiedRecordsTable = this.PrepareColumnListForTableVariable(tableColumns, this.IncludeOldValues);
            var columnsForExceptTable = this.PrepareColumnListForTableVariable(tableColumns, false);
            var columnsForDeletedTable = this.PrepareColumnListForTableVariable(tableColumns, false);

            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();

                using (var transaction = sqlConnection.BeginTransaction())
                {
                    var sqlCommand = new SqlCommand { Connection = sqlConnection, Transaction = transaction };

                    // Messages
                    var startMessageInsert = string.Format(StartMessageTemplate, _dataBaseObjectsNamingConvention, ChangeType.Insert);
                    sqlCommand.CommandText = $"CREATE MESSAGE TYPE [{startMessageInsert}] VALIDATION = NONE;";
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Message {startMessageInsert} created.");
                    processableMessages.Add(startMessageInsert);

                    var startMessageUpdate = string.Format(StartMessageTemplate, _dataBaseObjectsNamingConvention, ChangeType.Update);
                    sqlCommand.CommandText = $"CREATE MESSAGE TYPE [{startMessageUpdate}] VALIDATION = NONE;";
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Message {startMessageUpdate} created.");
                    processableMessages.Add(startMessageUpdate);

                    var startMessageDelete = string.Format(StartMessageTemplate, _dataBaseObjectsNamingConvention, ChangeType.Delete);
                    sqlCommand.CommandText = $"CREATE MESSAGE TYPE [{startMessageDelete}] VALIDATION = NONE;";
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Message {startMessageDelete} created.");
                    processableMessages.Add(startMessageDelete);

                    var interestedColumns = userInterestedColumns as TableColumnInfo[] ?? tableColumns.ToArray();
                    foreach (var userInterestedColumn in interestedColumns)
                    {
                        var message = $"{_dataBaseObjectsNamingConvention}/{userInterestedColumn.Name}";
                        sqlCommand.CommandText = $"CREATE MESSAGE TYPE [{message}] VALIDATION = NONE;";
                        sqlCommand.ExecuteNonQuery();
                        this.WriteTraceMessage(TraceLevel.Verbose, $"Message {message} created.");
                        processableMessages.Add(message);

                        if (this.IncludeOldValues)
                        {
                            message = $"{_dataBaseObjectsNamingConvention}/{userInterestedColumn.Name}/old";
                            sqlCommand.CommandText = $"CREATE MESSAGE TYPE [{message}] VALIDATION = NONE;";
                            sqlCommand.ExecuteNonQuery();
                            this.WriteTraceMessage(TraceLevel.Verbose, $"Message {message} created.");
                            processableMessages.Add(message);
                        }
                    }

                    var endMessage = string.Format(EndMessageTemplate, _dataBaseObjectsNamingConvention);
                    sqlCommand.CommandText = $"CREATE MESSAGE TYPE [{endMessage}] VALIDATION = NONE;";
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Message {endMessage} created.");
                    processableMessages.Add(endMessage);

                    // Contract
                    var contractBody = string.Join("," + Environment.NewLine, processableMessages.Select(message => $"[{message}] SENT BY INITIATOR"));
                    sqlCommand.CommandText = $"CREATE CONTRACT [{_dataBaseObjectsNamingConvention}] ({contractBody})";
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Contract {_dataBaseObjectsNamingConvention} created.");

                    // Queues
                    sqlCommand.CommandText = $"CREATE QUEUE [{_schemaName}].[{_dataBaseObjectsNamingConvention}_Receiver] WITH STATUS = ON, RETENTION = OFF, POISON_MESSAGE_HANDLING (STATUS = OFF);";
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Queue {_dataBaseObjectsNamingConvention}_Receiver created.");

                    sqlCommand.CommandText = $"CREATE QUEUE [{_schemaName}].[{_dataBaseObjectsNamingConvention}_Sender] WITH STATUS = ON, RETENTION = OFF, POISON_MESSAGE_HANDLING (STATUS = OFF);";
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Queue {_dataBaseObjectsNamingConvention}_Sender created.");

                    // Services
                    sqlCommand.CommandText = string.IsNullOrWhiteSpace(this.ServiceAuthorization)
                        ? $"CREATE SERVICE [{_dataBaseObjectsNamingConvention}_Sender] ON QUEUE [{_schemaName}].[{_dataBaseObjectsNamingConvention}_Sender];"
                        : $"CREATE SERVICE [{_dataBaseObjectsNamingConvention}_Sender] AUTHORIZATION [{this.ServiceAuthorization}] ON QUEUE [{_schemaName}].[{_dataBaseObjectsNamingConvention}_Sender];";
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Service broker {_dataBaseObjectsNamingConvention}_Sender created.");

                    sqlCommand.CommandText = string.IsNullOrWhiteSpace(this.ServiceAuthorization)
                        ? $"CREATE SERVICE [{_dataBaseObjectsNamingConvention}_Receiver] ON QUEUE [{_schemaName}].[{_dataBaseObjectsNamingConvention}_Receiver] ([{_dataBaseObjectsNamingConvention}]);"
                        : $"CREATE SERVICE [{_dataBaseObjectsNamingConvention}_Receiver] AUTHORIZATION [{this.ServiceAuthorization}] ON QUEUE [{_schemaName}].[{_dataBaseObjectsNamingConvention}_Receiver] ([{_dataBaseObjectsNamingConvention}]);";
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Service broker {_dataBaseObjectsNamingConvention}_Receiver created.");

                    // Activation Store Procedure
                    var dropMessages = string.Join(Environment.NewLine, processableMessages.Select((pm, index) =>
                    {
                        if (index > 0) return this.Spacer(8) + string.Format("IF EXISTS (SELECT * FROM sys.service_message_types WITH (NOLOCK) WHERE name = N'{0}') DROP MESSAGE TYPE [{0}];", pm);
                        return string.Format("IF EXISTS (SELECT * FROM sys.service_message_types WITH (NOLOCK) WHERE name = N'{0}') DROP MESSAGE TYPE [{0}];", pm);
                    }));

                    var dropAllScript = this.PrepareScriptDropAll(dropMessages);
                    sqlCommand.CommandText = this.PrepareScriptProcedureQueueActivation(dropAllScript);
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Procedure {_dataBaseObjectsNamingConvention} created.");

                    // Begin conversation
                    this.ConversationHandle = this.BeginConversation(sqlCommand);
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Conversation with handler {this.ConversationHandle} started.");

                    // Trigger
                    var declareVariableStatement = this.PrepareDeclareVariableStatement(interestedColumns);
                    var selectForSetVariablesStatement = this.PrepareSelectForSetVariables(interestedColumns);
                    var sendInsertConversationStatements = this.PrepareSendConversation(ChangeType.Insert, interestedColumns);
                    var sendUpdatedConversationStatements = this.PrepareSendConversation(ChangeType.Update, interestedColumns);
                    var sendDeletedConversationStatements = this.PrepareSendConversation(ChangeType.Delete, interestedColumns);

                    sqlCommand.CommandText = string.Format(
                        SqlScripts.CreateTrigger,
                        _dataBaseObjectsNamingConvention,
                        $"[{_schemaName}].[{_tableName}]",
                        columnsForModifiedRecordsTable,
                        this.PrepareColumnListForSelectFromTableVariable(tableColumns),
                        this.PrepareInsertIntoTableVariableForUpdateChange(interestedColumns, columnsForUpdateOf),
                        declareVariableStatement,
                        selectForSetVariablesStatement,
                        sendInsertConversationStatements,
                        sendUpdatedConversationStatements,
                        sendDeletedConversationStatements,
                        ChangeType.Insert,
                        ChangeType.Update,
                        ChangeType.Delete,
                        string.Join(", ", this.GetDmlTriggerType(_dmlTriggerType)),
                        this.CreateWhereCondition(),
                        this.PrepareTriggerLogScript(),
                        this.ActivateDatabaseLogging ? " WITH LOG" : string.Empty,
                        columnsForExceptTable,
                        columnsForDeletedTable,
                        this.ConversationHandle);

                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Trigger {_dataBaseObjectsNamingConvention} created.");

                    // Associate Activation Store Procedure to sender queue
                    sqlCommand.CommandText = $"ALTER QUEUE [{_schemaName}].[{_dataBaseObjectsNamingConvention}_Sender] WITH ACTIVATION (PROCEDURE_NAME = [{_schemaName}].[{_dataBaseObjectsNamingConvention}_QueueActivationSender], MAX_QUEUE_READERS = 1, EXECUTE AS {this.QueueExecuteAs.ToUpper()}, STATUS = ON);";
                    sqlCommand.ExecuteNonQuery();

                    if (_throwExceptionCreateSqlServerDatabaseObjects) throw new Exception();

                    // Run the watch-dog
                    sqlCommand.CommandText = $"BEGIN CONVERSATION TIMER ('{this.ConversationHandle.ToString().ToUpper()}') TIMEOUT = " + watchDogTimeOut + ";";
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, "Watch dog started.");

                    // Persist all objects
                    transaction.Commit();
                }

                _databaseObjectsCreated = true;

                this.WriteTraceMessage(TraceLevel.Info, $"All OK! Database objects created with naming {_dataBaseObjectsNamingConvention}.");
            }

            return processableMessages;
        }

        protected override async Task WaitForNotifications(
             CancellationToken cancellationToken,
             Delegate[] onChangeSubscribedList,
             Delegate[] onErrorSubscribedList,
             Delegate[] onStatusChangedSubscribedList,
             int timeOut,
             int timeOutWatchDog)
        {
            this.WriteTraceMessage(TraceLevel.Verbose, "Get in WaitForNotifications.");

            if (_throwExceptionInWaitForNotificationsPoint1) throw new Exception();

            var messagesBag = this.CreateMessagesBag(this.Encoding, _processableMessages);
            var messageNumber = _userInterestedColumns.Count() * (this.IncludeOldValues ? 2 : 1) + 2;

            var waitForSqlScript =
                $"BEGIN CONVERSATION TIMER ('{this.ConversationHandle.ToString().ToUpper()}') TIMEOUT = " + timeOutWatchDog + ";" +
                $"WAITFOR (RECEIVE TOP({messageNumber}) [message_type_name], [message_body] FROM [{_schemaName}].[{_dataBaseObjectsNamingConvention}_Receiver]), TIMEOUT {timeOut * 1000};";

            this.NotifyListenersAboutStatus(onStatusChangedSubscribedList, TableDependencyStatus.Started);

            try
            {
                using (var sqlConnection = new SqlConnection(_connectionString))
                {
                    await sqlConnection.OpenAsync(cancellationToken);
                    this.WriteTraceMessage(TraceLevel.Verbose, "Connection opened.");
                    this.NotifyListenersAboutStatus(onStatusChangedSubscribedList, TableDependencyStatus.WaitingForNotification);

                    while (true)
                    {
                        messagesBag.Reset();

                        using (var sqlCommand = new SqlCommand(waitForSqlScript, sqlConnection))
                        {
                            if (_throwExceptionInWaitForNotificationsPoint2) throw new Exception();

                            sqlCommand.CommandTimeout = 0;
                            this.WriteTraceMessage(TraceLevel.Verbose, "Executing WAITFOR command.");

                            using (var sqlDataReader = await sqlCommand.ExecuteReaderAsync(cancellationToken).WithCancellation(cancellationToken))
                            {
                                if (_throwExceptionInWaitForNotificationsPoint3) throw new Exception();

                                while (sqlDataReader.Read())
                                {
                                    var message = new Message(sqlDataReader.GetSqlString(0).Value, sqlDataReader.IsDBNull(1) ? null : sqlDataReader.GetSqlBytes(1).Value);
                                    if (message.MessageType == SqlMessageTypes.ErrorType) throw new QueueContainingErrorMessageException();
                                    messagesBag.AddMessage(message);
                                    this.WriteTraceMessage(TraceLevel.Verbose, $"Received message type = {message.MessageType}.");
                                }
                            }
                        }

                        if (messagesBag.Status == MessagesBagStatus.Collecting)
                        {
                            throw new MessageMisalignedException("Received a number of messages lower than expected.");
                        }

                        if (messagesBag.Status == MessagesBagStatus.Ready)
                        {
                            this.WriteTraceMessage(TraceLevel.Verbose, "Message ready to be notified.");
                            this.NotifyListenersAboutChange(onChangeSubscribedList, messagesBag);
                            this.WriteTraceMessage(TraceLevel.Verbose, "Message notified.");
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                this.NotifyListenersAboutStatus(onStatusChangedSubscribedList, TableDependencyStatus.StopDueToCancellation);
                this.WriteTraceMessage(TraceLevel.Info, "Operation canceled.");
            }
            catch (AggregateException aggregateException)
            {
                this.NotifyListenersAboutStatus(onStatusChangedSubscribedList, TableDependencyStatus.StopDueToError);
                if (cancellationToken.IsCancellationRequested == false) this.NotifyListenersAboutError(onErrorSubscribedList, aggregateException.InnerException);
                this.WriteTraceMessage(TraceLevel.Error, "Exception in WaitForNotifications.", aggregateException.InnerException);
            }
            catch (SqlException sqlException)
            {
                this.NotifyListenersAboutStatus(onStatusChangedSubscribedList, TableDependencyStatus.StopDueToError);
                if (cancellationToken.IsCancellationRequested == false) this.NotifyListenersAboutError(onErrorSubscribedList, sqlException);
                this.WriteTraceMessage(TraceLevel.Error, "Exception in WaitForNotifications.", sqlException);
            }
            catch (Exception exception)
            {
                this.NotifyListenersAboutStatus(onStatusChangedSubscribedList, TableDependencyStatus.StopDueToError);
                if (cancellationToken.IsCancellationRequested == false) this.NotifyListenersAboutError(onErrorSubscribedList, exception);
                this.WriteTraceMessage(TraceLevel.Error, "Exception in WaitForNotifications.", exception);
            }
        }

        public override void Stop()
        {
            if (_task != null)
            {
                _cancellationTokenSource.Cancel(true);
                _task?.Wait();
            }

            _task = null;

            if (!_stopWithoutDisposing) this.DropDatabaseObjects();

            _disposed = true;

            this.WriteTraceMessage(TraceLevel.Info, "Stopped waiting for notification.");
        }
    }
}