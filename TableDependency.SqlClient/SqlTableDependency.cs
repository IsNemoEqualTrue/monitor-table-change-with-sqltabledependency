#region License
// TableDependency, SqlTableDependency
// Copyright (c) 2015-2018 Christian Del Bianco. All rights reserved.
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

#region Usings
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TableDependency.Abstracts;
using TableDependency.Delegates;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Exceptions;
using TableDependency.Messages;
using TableDependency.SqlClient.Enumerations;
using TableDependency.SqlClient.Extensions;
using TableDependency.SqlClient.EventArgs;
using TableDependency.SqlClient.Exceptions;
using TableDependency.SqlClient.Messages;
using TableDependency.SqlClient.Resources;
using TableDependency.SqlClient.Utilities;
using TableDependency.Utilities;
#endregion

namespace TableDependency.SqlClient
{
    /// <summary>
    /// SqlTableDependency class.
    /// </summary>
    public class SqlTableDependency<T> : TableDependency<T> where T : class
    {
        #region Private variables

        protected Guid ConversationHandle;
        protected const string StartMessageTemplate = "{0}/StartMessage/{1}";
        protected const string EndMessageTemplate = "{0}/EndMessage";

        #endregion

        #region Properties

        /// <summary>
        /// Gets or sets a value indicating whether activate database loging and event viewer loging.
        /// </summary>
        /// <remarks>
        /// Only a member of the sysadmin fixed server role or a user with ALTER TRACE permissions can use it.
        /// </remarks>
        /// <value>
        /// <c>true</c> if [activate database loging]; otherwise, <c>false</c>.
        /// </value>
        public bool ActivateDatabaseLoging { get; set; }

        /// <summary>
        /// Specifies the owner of the service to the specified database user.
        /// When a new service is created it is owned by the principal specified in the AUTHORIZATION clause. Server, database, and schema names cannot be specified. The service_name must be a valid sysname.
        /// When the current user is dbo or sa, owner_name may be the name of any valid user or role.
        /// Otherwise, owner_name must be the name of the current user, the name of a user that the current user has IMPERSONATE permission for, or the name of a role to which the current user belongs.
        /// </summary>
        public string ServiceAuthorization { get; set; }

        /// <summary>
        /// Specifies the SQL Server database user account under which the activation stored procedure runs.
        /// SQL Server must be able to check the permissions for this user at the time that the queue activates the stored procedure. For aWindows domain user, the server must be connected to the domain
        /// when the procedure is activated or when activation fails.For a SQL Server user, Service Broker always checks the permissions.EXECUTE AS SELF means that the stored procedure executes as the current user.
        /// </summary>
        public string QueueExecuteAs { get; set; } = "SELF";

        #endregion

        #region Events

        /// <summary>
        /// Occurs when an error happen during listening for changes on monitored table.
        /// </summary>
        public override event ErrorEventHandler OnError;

        /// <summary>
        /// Occurs when the table content has been changed with an update, insert or delete operation.
        /// </summary>
        public override event ChangedEventHandler<T> OnChanged;

        /// <summary>
        /// Occurs when an status changes happen.
        /// </summary>
        public override event StatusEventHandler OnStatusChanged;

        #endregion

        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="schemaName">Name of the schema.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="mapper">The model to database table column mapper.</param>
        /// <param name="updateOf">List of columns that need to monitor for changing on order to receive notifications.</param>
        /// <param name="filter">The filter condition translated in WHERE.</param>
        /// <param name="notifyOn">The notify on Insert, Delete, Update operation.</param>
        /// <param name="executeUserPermissionCheck">if set to <c>true</c> [skip user permission check].</param>
        public SqlTableDependency(
            string connectionString,
            string schemaName = null,
            string tableName = null,
            IModelToTableMapper<T> mapper = null,
            IUpdateOfModel<T> updateOf = null,
            ITableDependencyFilter filter = null,
            DmlTriggerType notifyOn = DmlTriggerType.All,
            bool executeUserPermissionCheck = true) : base(connectionString, schemaName, tableName, mapper, updateOf, filter, notifyOn, executeUserPermissionCheck)
        {
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
        public override void Start(int timeOut = 120, int watchDogTimeOut = 180)
        {
            if (OnChanged == null) throw new NoSubscriberException();

            var onChangedSubscribedList = OnChanged?.GetInvocationList();
            var onErrorSubscribedList = OnError?.GetInvocationList();
            var onStatusChangedSubscribedList = OnStatusChanged?.GetInvocationList();

            this.NotifyListenersAboutStatus(onStatusChangedSubscribedList, TableDependencyStatus.Starting);

            base.Start(timeOut, watchDogTimeOut);

            _cancellationTokenSource = new CancellationTokenSource();

            _task = Task.Factory.StartNew(() =>
                WaitForNotifications(
                    _cancellationTokenSource.Token,
                    onChangedSubscribedList,
                    onErrorSubscribedList,
                    onStatusChangedSubscribedList,
                    this.ConversationHandle,
                    _connectionString,
                    _schemaName,
                    _dataBaseObjectsNamingConvention,
                    timeOut,
                    watchDogTimeOut,
                    _processableMessages,
                    _mapper,
                    _userInterestedColumns,
                    this.Encoding),
                _cancellationTokenSource.Token);

            this.WriteTraceMessage(TraceLevel.Info, $"Waiting for receiving {_tableName}'s records change notifications.");
        }

        #endregion

        #region protected virtual methods

        protected override RecordChangedEventArgs<T> GetRecordChangedEventArgs(MessagesBag messagesBag)
        {
            return new SqlRecordChangedEventArgs<T>(
                messagesBag,
                _mapper,
                _userInterestedColumns,
                _server,
                _database,
                _dataBaseObjectsNamingConvention,
                base.CultureInfo);
        }

        protected override string GetDataBaseName(string connectionString)
        {
            var sqlConnectionStringBuilder = new SqlConnectionStringBuilder(connectionString);
            return sqlConnectionStringBuilder.InitialCatalog;
        }

        protected override string GetServerName(string connectionString)
        {
            var sqlConnectionStringBuilder = new SqlConnectionStringBuilder(connectionString);
            return sqlConnectionStringBuilder.DataSource;
        }

        protected override string GetTableName(string tableName)
        {
            if (!string.IsNullOrWhiteSpace(tableName))
            {
                return tableName.Replace("[", string.Empty).Replace("]", string.Empty);
            }

            var tableNameFromDataAnotation = GetTableNameFromDataAnnotation();
            return !string.IsNullOrWhiteSpace(tableNameFromDataAnotation) ? tableNameFromDataAnotation : typeof(T).Name;
        }

        protected override string GetSchemaName(string schemaName)
        {
            if (!string.IsNullOrWhiteSpace(schemaName))
            {
                return schemaName.Replace("[", string.Empty).Replace("]", string.Empty);
            }

            var schemaNameFromDataAnnotation = GetSchemaNameFromDataAnnotation();
            return !string.IsNullOrWhiteSpace(schemaNameFromDataAnnotation) ? schemaNameFromDataAnnotation : "dbo";
        }

        protected virtual int GetSchemaId(string schemaName, string connectionString)
        {
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"SELECT [schema_id] FROM [sys].[schemas] WITH (NOLOCK) WHERE [name] = '{schemaName}'";
                    return (int)sqlCommand.ExecuteScalar();
                }
            }
        }

        protected virtual SqlServerVersion GetSqlServerVersion(string connectionString)
        {
            var sqlConnection = new SqlConnection(connectionString);

            try
            {
                sqlConnection.Open();

                var serverVersion = sqlConnection.ServerVersion;
                var serverVersionDetails = serverVersion.Split(new[] { "." }, StringSplitOptions.None);

                var versionNumber = int.Parse(serverVersionDetails[0]);
                if (versionNumber < 8) return SqlServerVersion.Unknown;
                if (versionNumber == 8) return SqlServerVersion.SqlServer2000;
                if (versionNumber == 9) return SqlServerVersion.SqlServer2005;
                if (versionNumber == 10) return SqlServerVersion.SqlServer2008;
                if (versionNumber == 11) return SqlServerVersion.SqlServer2012;
            }
            catch
            {
                throw new SqlServerVersionNotSupportedException();
            }
            finally
            {
                sqlConnection.Close();
            }

            return SqlServerVersion.SqlServerLatest;
        }

        protected override IEnumerable<ColumnInfo> GetTableColumnsList(string connectionString)
        {
            var columnsList = new List<ColumnInfo>();

            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = string.Format(SqlScripts.InformationSchemaColumns, _schemaName, _tableName);
                    var reader = sqlCommand.ExecuteReader();
                    while (reader.Read())
                    {
                        var name = reader["COLUMN_NAME"].ToString();
                        var type = reader["DATA_TYPE"].ToString().ConvertNumericType();
                        var size = this.ComputeSize(
                            type,
                            reader.GetSafeString(reader.GetOrdinal("CHARACTER_MAXIMUM_LENGTH")),
                            reader.GetSafeString(reader.GetOrdinal("NUMERIC_PRECISION")),
                            reader.GetSafeString(reader.GetOrdinal("NUMERIC_SCALE")),
                            reader.GetSafeString(reader.GetOrdinal("DATETIME_PRECISION")));

                        columnsList.Add(new ColumnInfo(name, type, size));
                    }
                }
            }

            return columnsList;
        }

        protected virtual bool CheckIfDatabaseObjectExists(string connectionString)
        {
            bool result;

            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                var sqlCommand = new SqlCommand($"SELECT COUNT(*) FROM sys.service_queues WITH (NOLOCK) WHERE name = N'{_dataBaseObjectsNamingConvention}';", sqlConnection);
                result = (int)sqlCommand.ExecuteScalar() > 0;
                sqlConnection.Close();
            }

            return result;
        }

        protected override IList<string> CreateDatabaseObjects(string connectionString, string tableName, string dataBaseObjectsNamingConvention, IEnumerable<ColumnInfo> userInterestedColumns, IList<string> updateOf, int timeOut, int watchDogTimeOut)
        {
            IList<string> processableMessages;

            var interestedColumns = userInterestedColumns as ColumnInfo[] ?? userInterestedColumns.ToArray();

            if (this.CheckIfDatabaseObjectExists(connectionString) == false)
            {
                var columnsForTableVariable = this.PrepareColumnListForTableVariable(interestedColumns);
                var columnsForSelect = string.Join(",", interestedColumns.Select(c => $"[{c.Name}]").ToList());
                var columnsForUpdateOf = _updateOf != null ? string.Join(" OR ", _updateOf.Where(c => !string.IsNullOrWhiteSpace(c)).Distinct(StringComparer.CurrentCultureIgnoreCase).Select(c => $"UPDATE([{c}])").ToList()) : null;
                processableMessages = this.CreateSqlServerDatabaseObjects(connectionString, dataBaseObjectsNamingConvention, interestedColumns, columnsForTableVariable, columnsForSelect, columnsForUpdateOf, watchDogTimeOut);
            }
            else
            {
                throw new DbObjectsWithSameNameException(_dataBaseObjectsNamingConvention);
            }

            return processableMessages;
        }

        protected override string GetBaseObjectsNamingConvention()
        {
            var name = $"{_schemaName}_{_tableName}";
            return $"{name}_{Guid.NewGuid()}";
        }

        protected override void DropDatabaseObjects(string connectionString, string databaseObjectsNaming)
        {
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                this.DropDatabaseObjects(sqlConnection, databaseObjectsNaming);
            }

            this.WriteTraceMessage(TraceLevel.Info, "DropDatabaseObjects done.");
        }

        protected virtual void DropDatabaseObjects(SqlConnection sqlConnection, string databaseObjectsNaming)
        {
            using (var sqlCommand = sqlConnection.CreateCommand())
            {
                var dropMessages = string.Join(Environment.NewLine, _processableMessages.Select(pm => string.Format("IF EXISTS (SELECT * FROM sys.service_message_types WITH (NOLOCK) WHERE name = N'{0}') DROP MESSAGE TYPE [{0}];", pm)));
                var dropAllScript = this.PrepareScriptDropAll(databaseObjectsNaming, dropMessages);

                sqlCommand.CommandType = CommandType.Text;
                sqlCommand.CommandText = dropAllScript;
                sqlCommand.ExecuteNonQuery();
            }
        }

        protected override void CheckRdbmsDependentImplementation(string connectionString)
        {
            this.CheckIfServiceBrokerIsEnabled(connectionString);

            var sqlVersion = this.GetSqlServerVersion(connectionString);
            if (sqlVersion < SqlServerVersion.SqlServer2008) throw new SqlServerVersionNotSupportedException(sqlVersion);
        }

        protected virtual string CreateWhereCondifition(bool prependSpace = false)
        {
            var where = string.Empty;

            var filter = _filter?.Translate();
            if (!string.IsNullOrWhiteSpace(filter)) where = (prependSpace ? " " : string.Empty) + "WHERE " + filter;

            return where;
        }

        protected virtual IList<string> CreateSqlServerDatabaseObjects(string connectionString, string databaseObjectsNaming, IEnumerable<ColumnInfo> userInterestedColumns, string tableColumns, string selectColumns, string updateColumns, int watchDogTimeOut)
        {
            var processableMessages = new List<string>();

            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();

                using (var transaction = sqlConnection.BeginTransaction())
                {
                    var sqlCommand = new SqlCommand($"SELECT COUNT(*) FROM sys.service_queues WITH (NOLOCK) WHERE name LIKE N'%{databaseObjectsNaming}%';", sqlConnection, transaction);
                    if ((int)sqlCommand.ExecuteScalar() > 0) throw new DbObjectsWithSameNameException(databaseObjectsNaming);

                    // Messages
                    var startMessageInsert = string.Format(StartMessageTemplate, databaseObjectsNaming, ChangeType.Insert);
                    sqlCommand.CommandText = $"CREATE MESSAGE TYPE [{startMessageInsert}] VALIDATION = NONE;";
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Message {startMessageInsert} created.");
                    processableMessages.Add(startMessageInsert);

                    var startMessageUpdate = string.Format(StartMessageTemplate, databaseObjectsNaming, ChangeType.Update);
                    sqlCommand.CommandText = $"CREATE MESSAGE TYPE [{startMessageUpdate}] VALIDATION = NONE;";
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Message {startMessageUpdate} created.");
                    processableMessages.Add(startMessageUpdate);

                    var startMessageDelete = string.Format(StartMessageTemplate, databaseObjectsNaming, ChangeType.Delete);
                    sqlCommand.CommandText = $"CREATE MESSAGE TYPE [{startMessageDelete}] VALIDATION = NONE;";
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Message {startMessageDelete} created.");
                    processableMessages.Add(startMessageDelete);

                    var interestedColumns = userInterestedColumns as ColumnInfo[] ?? userInterestedColumns.ToArray();
                    foreach (var userInterestedColumn in interestedColumns)
                    {
                        var message = $"{databaseObjectsNaming}/{userInterestedColumn.Name}";
                        sqlCommand.CommandText = $"CREATE MESSAGE TYPE [{message}] VALIDATION = NONE;";
                        sqlCommand.ExecuteNonQuery();
                        this.WriteTraceMessage(TraceLevel.Verbose, $"Message {message} created.");
                        processableMessages.Add(message);
                    }

                    var endMessage = string.Format(EndMessageTemplate, databaseObjectsNaming);
                    sqlCommand.CommandText = $"CREATE MESSAGE TYPE [{endMessage}] VALIDATION = NONE;";
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Message {endMessage} created.");
                    processableMessages.Add(endMessage);

                    // Contract
                    var contractBody = string.Join("," + Environment.NewLine, processableMessages.Select(message => $"[{message}] SENT BY INITIATOR"));
                    sqlCommand.CommandText = $"CREATE CONTRACT [{databaseObjectsNaming}] ({contractBody})";
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Contract {databaseObjectsNaming} created.");

                    // Queues
                    sqlCommand.CommandText = $"CREATE QUEUE [{_schemaName}].[{databaseObjectsNaming}_Receiver] WITH STATUS = ON, RETENTION = OFF, POISON_MESSAGE_HANDLING (STATUS = OFF);";
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Queue {databaseObjectsNaming}_Receiver created.");

                    sqlCommand.CommandText = $"CREATE QUEUE [{_schemaName}].[{databaseObjectsNaming}_Sender] WITH STATUS = ON, RETENTION = OFF, POISON_MESSAGE_HANDLING (STATUS = OFF);";
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Queue {databaseObjectsNaming}_Sender created.");

                    // Services
                    sqlCommand.CommandText = string.IsNullOrWhiteSpace(this.ServiceAuthorization)
                        ? $"CREATE SERVICE [{databaseObjectsNaming}_Sender] ON QUEUE [{_schemaName}].[{databaseObjectsNaming}_Sender];"
                        : $"CREATE SERVICE [{databaseObjectsNaming}_Sender] AUTHORIZATION [{this.ServiceAuthorization}] ON QUEUE [{_schemaName}].[{databaseObjectsNaming}_Sender];";
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Service broker {databaseObjectsNaming}_Sender created.");

                    sqlCommand.CommandText = string.IsNullOrWhiteSpace(this.ServiceAuthorization)
                        ? $"CREATE SERVICE [{databaseObjectsNaming}_Receiver] ON QUEUE [{_schemaName}].[{databaseObjectsNaming}_Receiver] ([{databaseObjectsNaming}]);"
                        : $"CREATE SERVICE [{databaseObjectsNaming}_Receiver] AUTHORIZATION [{this.ServiceAuthorization}] ON QUEUE [{_schemaName}].[{databaseObjectsNaming}_Receiver] ([{databaseObjectsNaming}]);";
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Service broker {databaseObjectsNaming}_Receiver created.");

                    // Activation Store Procedure
                    var dropMessages = string.Join(Environment.NewLine, processableMessages.Select(pm => string.Format("IF EXISTS (SELECT * FROM sys.service_message_types WITH (NOLOCK) WHERE name = N'{0}') DROP MESSAGE TYPE [{0}];", pm)));
                    var dropAllScript = this.PrepareScriptDropAll(databaseObjectsNaming, dropMessages);
                    sqlCommand.CommandText = this.PrepareScriptProcedureQueueActivation(databaseObjectsNaming, dropAllScript);
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Procedure {databaseObjectsNaming} created.");

                    // Begin conversation
                    this.ConversationHandle = this.BeginConversation(sqlCommand, databaseObjectsNaming);
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Conversation with handler {this.ConversationHandle} started.");

                    // Trigger
                    var declareVariableStatement = this.PrepareDeclareVariableStatement(interestedColumns);
                    var selectForSetVariablesStatement = this.PrepareSelectForSetVariables(interestedColumns);
                    var sendInsertConversationStatements = this.PrepareSendConversation(databaseObjectsNaming, ChangeType.Insert, interestedColumns);
                    var sendUpdatedConversationStatements = this.PrepareSendConversation(databaseObjectsNaming, ChangeType.Update, interestedColumns);
                    var sendDeletedConversationStatements = this.PrepareSendConversation(databaseObjectsNaming, ChangeType.Delete, interestedColumns);
                    var exceptStatement = this.PrepareExceptStatement(interestedColumns);
                    var bodyForUpdate = !string.IsNullOrEmpty(updateColumns)
                        ? string.Format(SqlScripts.TriggerUpdateWithColumns, updateColumns, selectColumns, ChangeType.Update, exceptStatement)
                        : string.Format(SqlScripts.TriggerUpdateWithoutColumns, selectColumns, ChangeType.Update, exceptStatement);

                    sqlCommand.CommandText = this.PrepareScriptTrigger(
                        databaseObjectsNaming,
                        tableColumns,
                        selectColumns,
                        bodyForUpdate,
                        declareVariableStatement,
                        selectForSetVariablesStatement,
                        sendInsertConversationStatements,
                        sendUpdatedConversationStatements,
                        sendDeletedConversationStatements);

                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, $"Trigger {databaseObjectsNaming} created.");

                    // Associate Activation Store Procedure to sender queue
                    sqlCommand.CommandText = $"ALTER QUEUE [{_schemaName}].[{databaseObjectsNaming}_Sender] WITH ACTIVATION (PROCEDURE_NAME = [{_schemaName}].[{databaseObjectsNaming}_QueueActivationSender], MAX_QUEUE_READERS = 1, EXECUTE AS {this.QueueExecuteAs.ToUpper()}, STATUS = ON);";
                    sqlCommand.ExecuteNonQuery();

                    // Persist all objects
                    transaction.Commit();
                }

                this.WriteTraceMessage(TraceLevel.Info, $"Database objects created with naming {databaseObjectsNaming}.");
            }

            return processableMessages;
        }

        protected virtual Guid BeginConversation(SqlCommand sqlCommand, string databaseObjectsNaming)
        {
            sqlCommand.CommandText = $"DECLARE @h AS UNIQUEIDENTIFIER; BEGIN DIALOG CONVERSATION @h FROM SERVICE [{databaseObjectsNaming}_Sender] TO SERVICE '{databaseObjectsNaming}_Receiver' ON CONTRACT [{databaseObjectsNaming}] WITH ENCRYPTION = OFF; SELECT @h;";
            var conversationHandler = (Guid)sqlCommand.ExecuteScalar();
            if (conversationHandler == Guid.Empty) throw new ServiceBrokerConversationHandlerInvalidException();

            return conversationHandler;
        }

        protected virtual string PrepareScriptTrigger(
            string databaseObjectsNaming,
            string tableColumns,
            string selectColumns,
            string bodyForUpdate,
            string declareVariableStatement,
            string selectForSetVariablesStatement,
            string sendInsertConversationStatements,
            string sendUpdatedConversationStatements,
            string sendDeletedConversationStatements)
        {
            return string.Format(
                 SqlScripts.CreateTrigger,
                 databaseObjectsNaming,
                 $"[{_schemaName}].[{_tableName}]",
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
                 ChangeType.Delete,
                 string.Join(",", this.GetDmlTriggerType(_dmlTriggerType)),
                 this.CreateWhereCondifition(),
                 this.PrepareTriggerLogScript(databaseObjectsNaming),
                 this.ActivateDatabaseLoging ? " WITH LOG" : string.Empty);
        }

        protected virtual string PrepareTriggerLogScript(string databaseObjectsNaming)
        {
            if (this.ActivateDatabaseLoging == false) return string.Empty;

            return
                "DECLARE @LogMessage varchar(255);" + Environment.NewLine +
                $"SET @LogMessage = 'SqlTableDependency: Message for ' + @dmlType + ' operation added in Queue [{databaseObjectsNaming}].'" + Environment.NewLine +
                "RAISERROR(@LogMessage, 10, 1) WITH LOG;";
        }

        protected virtual string PrepareScriptProcedureQueueActivation(string databaseObjectsNaming, string dropAllScript)
        {
            var script = string.Format(SqlScripts.CreateProcedureQueueActivation, databaseObjectsNaming, dropAllScript, _schemaName);
            return this.ActivateDatabaseLoging ? script : this.RemoveLogOperations(script);
        }

        protected virtual string PrepareScriptDropAll(string databaseObjectsNaming, string dropMessages)
        {
            var script = string.Format(SqlScripts.ScriptDropAll, databaseObjectsNaming, dropMessages, _schemaName);
            return this.ActivateDatabaseLoging ? script : this.RemoveLogOperations(script);
        }

        protected virtual string RemoveLogOperations(string source)
        {
            while (true)
            {
                var startPos = source.IndexOf("PRINT N'SqlTableDependency:", StringComparison.InvariantCultureIgnoreCase);
                if (startPos < 1) break;

                var endPos = source.IndexOf(".';", startPos, StringComparison.InvariantCultureIgnoreCase);
                if (endPos < 1) break;

                source = source.Substring(0, startPos) + source.Substring(endPos + ".';".Length);
            }

            return source;
        }

        protected virtual string PrepareExceptStatement(IReadOnlyCollection<ColumnInfo> interestedColumns)
        {
            if (interestedColumns.Any(tableColumn =>
                string.Equals(tableColumn.Type.ToLowerInvariant(), "timestamp", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(tableColumn.Type.ToLowerInvariant(), "rowversion", StringComparison.OrdinalIgnoreCase))) return "INSERTED";

            var separatorNewColumns = new Separator(2, ",");
            var sBuilderNewColumns = new StringBuilder();
            var separatorOldColumns = new Separator(2, ",");
            var sBuilderOldColumns = new StringBuilder();

            foreach (var column in interestedColumns)
            {
                sBuilderNewColumns.Append($"{separatorNewColumns.GetSeparator()}[m_New].[{column.Name}]");
                sBuilderOldColumns.Append($"{separatorOldColumns.GetSeparator()}[m_Old].[{column.Name}]");
            }

            var exceptStatement = $"(SELECT {sBuilderNewColumns} FROM INSERTED AS [m_New] EXCEPT SELECT {sBuilderOldColumns} FROM DELETED AS [m_Old]) a";

            exceptStatement += CreateWhereCondifition(true);

            return exceptStatement;
        }

        protected virtual IEnumerable<string> GetDmlTriggerType(DmlTriggerType dmlTriggerType)
        {
            var afters = new List<string>();
            if (dmlTriggerType.HasFlag(DmlTriggerType.All))
            {
                afters.Add(DmlTriggerType.Insert.ToString().ToLowerInvariant());
                afters.Add(DmlTriggerType.Update.ToString().ToLowerInvariant());
                afters.Add(DmlTriggerType.Delete.ToString().ToLowerInvariant());
            }
            else
            {
                if (dmlTriggerType.HasFlag(DmlTriggerType.Insert)) afters.Add(DmlTriggerType.Insert.ToString().ToLowerInvariant());
                if (dmlTriggerType.HasFlag(DmlTriggerType.Delete)) afters.Add(DmlTriggerType.Delete.ToString().ToLowerInvariant());
                if (dmlTriggerType.HasFlag(DmlTriggerType.Update)) afters.Add(DmlTriggerType.Update.ToString().ToLowerInvariant());
            }

            return afters;
        }

        protected virtual MessagesBag CreateMessagesBag(string databaseObjectsNaming, Encoding encoding, ICollection<string> processableMessages)
        {
            return new MessagesBag(
                encoding ?? Encoding.Unicode,
                new List<string> { string.Format(StartMessageTemplate, databaseObjectsNaming, ChangeType.Insert), string.Format(StartMessageTemplate, databaseObjectsNaming, ChangeType.Update), string.Format(StartMessageTemplate, databaseObjectsNaming, ChangeType.Delete) },
                string.Format(EndMessageTemplate, databaseObjectsNaming),
                processableMessages);
        }

        protected virtual string PrepareColumnListForTableVariable(IEnumerable<ColumnInfo> tableColumns)
        {
            var columns = tableColumns.Select(tableColumn =>
            {
                if (string.Equals(tableColumn.Type.ToLowerInvariant(), "timestamp", StringComparison.OrdinalIgnoreCase))
                {
                    return $"[{tableColumn.Name}] binary(8)";
                }

                if (string.Equals(tableColumn.Type.ToLowerInvariant(), "rowversion", StringComparison.OrdinalIgnoreCase))
                {
                    return $"[{tableColumn.Name}] varbinary(8)";
                }

                if (!string.IsNullOrWhiteSpace(tableColumn.Size))
                {
                    return $"[{tableColumn.Name}] {tableColumn.Type}({tableColumn.Size})";
                }

                return $"[{tableColumn.Name}] {tableColumn.Type}";
            });

            return string.Join(",", columns.ToList());
        }

        protected virtual string ComputeSize(string dataType, string characterMaximumLength, string numericPrecision, string numericScale, string dateTimePrecisione)
        {
            if (
                string.Equals(dataType.ToUpperInvariant(), "BINARY", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(dataType.ToUpperInvariant(), "VARBINARY", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(dataType.ToUpperInvariant(), "CHAR", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(dataType.ToUpperInvariant(), "NCHAR", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(dataType.ToUpperInvariant(), "VARCHAR", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(dataType.ToUpperInvariant(), "NVARCHAR", StringComparison.OrdinalIgnoreCase))
            {
                return characterMaximumLength == "-1" ? "MAX" : characterMaximumLength;
            }

            if (string.Equals(dataType.ToUpperInvariant(), "DECIMAL", StringComparison.OrdinalIgnoreCase))
            {
                return $"{numericPrecision},{numericScale}";
            }

            if (string.Equals(dataType.ToUpperInvariant(), "DATETIME2", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(dataType.ToUpperInvariant(), "DATETIMEOFFSET", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(dataType.ToUpperInvariant(), "TIME", StringComparison.OrdinalIgnoreCase))
            {
                return $"{dateTimePrecisione}";
            }

            return null;
        }

        protected override void CheckIfUserInterestedColumnsCanBeManaged(IEnumerable<ColumnInfo> tableColumnsToUse)
        {
            var checkIfUserInterestedColumnsCanBeManaged = tableColumnsToUse as ColumnInfo[] ?? tableColumnsToUse.ToArray();
            foreach (var tableColumn in checkIfUserInterestedColumnsCanBeManaged)
            {
                if (
                    string.Equals(tableColumn.Type.ToUpperInvariant(), "XML", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(tableColumn.Type.ToUpperInvariant(), "IMAGE", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(tableColumn.Type.ToUpperInvariant(), "TEXT", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(tableColumn.Type.ToUpperInvariant(), "NTEXT", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(tableColumn.Type.ToUpperInvariant(), "STRUCTURED", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(tableColumn.Type.ToUpperInvariant(), "GEOGRAPHY", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(tableColumn.Type.ToUpperInvariant(), "GEOMETRY", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(tableColumn.Type.ToUpperInvariant(), "HIERARCHYID", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(tableColumn.Type.ToUpperInvariant(), "SQL_VARIANT", StringComparison.OrdinalIgnoreCase))
                {
                    throw new ColumnTypeNotSupportedException($"{tableColumn.Type} column type is not an supported by SqlTableDependency.");
                }
            }
        }

        protected virtual string ConvertFormat(ColumnInfo userInterestedColumn)
        {
            return string.Equals(userInterestedColumn.Type, "datetime", StringComparison.OrdinalIgnoreCase) || string.Equals(userInterestedColumn.Type, "date", StringComparison.OrdinalIgnoreCase) ? ", 121" : string.Empty;
        }

        protected virtual string ConvertValueByType(IReadOnlyCollection<ColumnInfo> userInterestedColumns, ColumnInfo userInterestedColumn)
        {
            if (string.Equals(userInterestedColumn.Type, "binary", StringComparison.OrdinalIgnoreCase) || string.Equals(userInterestedColumn.Type, "varbinary", StringComparison.OrdinalIgnoreCase) || string.Equals(userInterestedColumn.Type, "timestamp", StringComparison.OrdinalIgnoreCase))
            {
                return SanitizeVariableName(userInterestedColumns, userInterestedColumn.Name);
            }

            return $"CONVERT(NVARCHAR(MAX), {SanitizeVariableName(userInterestedColumns, userInterestedColumn.Name)}{ConvertFormat(userInterestedColumn)})";
        }

        protected virtual string PrepareSendConversation(string databaseObjectsNaming, ChangeType dmlType, IReadOnlyCollection<ColumnInfo> userInterestedColumns)
        {
            var sendList = userInterestedColumns
                .Select(insterestedColumn => $"IF {SanitizeVariableName(userInterestedColumns, insterestedColumn.Name)} IS NOT NULL BEGIN" + Environment.NewLine + $";SEND ON CONVERSATION '{this.ConversationHandle}' MESSAGE TYPE [{databaseObjectsNaming}/{insterestedColumn.Name}] ({ConvertValueByType(userInterestedColumns, insterestedColumn)})" + Environment.NewLine + "END" + Environment.NewLine + "ELSE BEGIN" + Environment.NewLine + $";SEND ON CONVERSATION '{this.ConversationHandle}' MESSAGE TYPE [{databaseObjectsNaming}/{insterestedColumn.Name}] (0x)" + Environment.NewLine + "END")
                .ToList();

            sendList.Insert(0, $";SEND ON CONVERSATION '{this.ConversationHandle}' MESSAGE TYPE [{string.Format(StartMessageTemplate, databaseObjectsNaming, dmlType)}] (CONVERT(NVARCHAR, @dmlType))" + Environment.NewLine);
            sendList.Add($";SEND ON CONVERSATION '{this.ConversationHandle}' MESSAGE TYPE [{string.Format(EndMessageTemplate, databaseObjectsNaming)}] (0x)" + Environment.NewLine);

            return string.Join(Environment.NewLine, sendList);
        }

        protected virtual string PrepareSelectForSetVariables(IReadOnlyCollection<ColumnInfo> userInterestedColumns)
        {
            return string.Join(",", userInterestedColumns.Select(insterestedColumn => $"{SanitizeVariableName(userInterestedColumns, insterestedColumn.Name)} = [{insterestedColumn.Name}]"));
        }

        protected virtual string PrepareDeclareVariableStatement(IReadOnlyCollection<ColumnInfo> interestedColumns)
        {
            var colonne = (from insterestedColumn in interestedColumns
                           let variableType = $"{insterestedColumn.Type.ToLowerInvariant()}" + (string.IsNullOrWhiteSpace(insterestedColumn.Size)
                           ? string.Empty
                           : $"({insterestedColumn.Size})")
                           select $"DECLARE {SanitizeVariableName(interestedColumns, insterestedColumn.Name)} {variableType.ToLowerInvariant()}").ToList();

            return string.Join(Environment.NewLine, colonne);
        }

        protected virtual string SanitizeVariableName(IReadOnlyCollection<ColumnInfo> userInterestedColumns, string tableColumnName)
        {
            for (var i = 0; i < userInterestedColumns.Count; i++)
            {
                if (userInterestedColumns.ElementAt(i).Name == tableColumnName)
                {
                    return "@var" + (i + 1);
                }
            }

            throw new SanitizeVariableNameException(tableColumnName);
        }

        protected override void CheckIfConnectionStringIsValid(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException(nameof(connectionString));

            SqlConnectionStringBuilder sqlConnectionStringBuilder = null;

            try
            {
                sqlConnectionStringBuilder = new SqlConnectionStringBuilder(connectionString);
            }
            catch (Exception exception)
            {
                throw new InvalidConnectionStringException(connectionString, exception);
            }

            using (var sqlConnection = new SqlConnection(sqlConnectionStringBuilder.ConnectionString))
            {
                try
                {
                    sqlConnection.Open();
                }
                catch (SqlException exception)
                {
                    throw new ImpossibleOpenSqlConnectionException(sqlConnectionStringBuilder.ConnectionString, exception);
                }
            }
        }

        protected override void CheckIfUserHasPermissions(string connectionString)
        {
            PrivilegesTable privilegesTable;

            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = SqlScripts.SelectUserGrants;

                    var rows = SerializeSqlDataReader.Serialize(sqlCommand.ExecuteReader(CommandBehavior.CloseConnection));
                    privilegesTable = PrivilegesTable.FromEnumerable(rows);
                }
            }
            if (privilegesTable.Rows.Count == 0) throw new UserWithNoPermissionException();

            if (privilegesTable.Rows.Any(r => string.Equals(r.Role, "db_owner", StringComparison.OrdinalIgnoreCase)))
            {
                // Ok
            }
            else
            {
                foreach (var permission in Enum.GetValues(typeof(SqlServerRequiredPermission)))
                {
                    var permissionToCkeck = EnumUtil.GetDescriptionFromEnumValue((SqlServerRequiredPermission)permission);
                    if (privilegesTable.Rows.All(r => !string.Equals(r.PermissionType, permissionToCkeck, StringComparison.OrdinalIgnoreCase)))
                    {
                        throw new UserWithMissingPermissionException(permissionToCkeck);
                    }
                }
            }
        }

        protected virtual void CheckIfServiceBrokerIsEnabled(string connectionString)
        {
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = "SELECT is_broker_enabled FROM sys.databases WITH (NOLOCK) WHERE database_id = db_id();";
                    if ((bool)sqlCommand.ExecuteScalar() == false) throw new ServiceBrokerNotEnabledException();
                }
            }
        }

        protected override void CheckIfTableExists(string connection)
        {
            using (var sqlConnection = new SqlConnection(connection))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = string.Format(SqlScripts.InformationSchemaTables, _tableName, _schemaName);
                    if ((int)sqlCommand.ExecuteScalar() == 0) throw new NotExistingTableException(_tableName);
                }
            }
        }

        protected virtual async Task WaitForNotifications(
            CancellationToken cancellationToken,
            Delegate[] onChangeSubscribedList,
            Delegate[] onErrorSubscribedList,
            Delegate[] onStatusChangedSubscribedList,
            Guid conversationHandle,
            string connectionString,
            string schemaName,
            string databaseObjectsNaming,
            int timeOut,
            int timeOutWatchDog,
            ICollection<string> processableMessages,
            IModelToTableMapper<T> modelMapper,
            IEnumerable<ColumnInfo> userInterestedColumns,
            Encoding encoding)
        {
            this.WriteTraceMessage(TraceLevel.Verbose, "Get in WaitForNotifications.");

            var messagesBag = this.CreateMessagesBag(databaseObjectsNaming, encoding, processableMessages);

            var waitforSqlScript =
                $"BEGIN CONVERSATION TIMER ('{conversationHandle.ToString().ToUpper()}') TIMEOUT = " + timeOutWatchDog + ";" +
                $"WAITFOR (RECEIVE TOP({userInterestedColumns.Count() + 2}) [message_type_name], [message_body] FROM [{schemaName}].[{databaseObjectsNaming}_Receiver]), TIMEOUT {timeOut * 1000};";

            this.NotifyListenersAboutStatus(onStatusChangedSubscribedList, TableDependencyStatus.Started);

            try
            {
                using (var sqlConnection = new SqlConnection(connectionString))
                {
                    await sqlConnection.OpenAsync(cancellationToken);
                    this.WriteTraceMessage(TraceLevel.Verbose, "Connection opened.");
                    this.NotifyListenersAboutStatus(onStatusChangedSubscribedList, TableDependencyStatus.WaitingForNotification);

                    while (true)
                    {
                        messagesBag.Reset();

                        using (var sqlCommand = new SqlCommand(waitforSqlScript, sqlConnection))
                        {
                            sqlCommand.CommandTimeout = 0;
                            this.WriteTraceMessage(TraceLevel.Verbose, "Executing WAITFOR command.");

                            using (var sqlDataReader = await sqlCommand.ExecuteReaderAsync(cancellationToken).WithCancellation(cancellationToken))
                            {
                                while (sqlDataReader.Read())
                                {
                                    var message = new Message(sqlDataReader.GetSqlString(0).Value, sqlDataReader.IsDBNull(1) ? null : sqlDataReader.GetSqlBytes(1).Value);
                                    if (message.Recipient == SqlMessageTypes.ErrorType) throw new QueueContainingErrorMessageException();
                                    messagesBag.AddMessage(message);
                                    this.WriteTraceMessage(TraceLevel.Verbose, $"Received message type = {sqlDataReader.GetSqlString(0).Value}.");
                                }
                            }
                        }

                        if (messagesBag.Status == MessagesBagStatus.Collecting)
                        {
                            throw new MessageMisalignedException("Received a number of message lower than expected.");
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
    }

    #endregion
}