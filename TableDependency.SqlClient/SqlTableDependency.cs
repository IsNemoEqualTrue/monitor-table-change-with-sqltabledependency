#region License
// TableDependency, SqlTableDependency
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

#region Usings
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TableDependency.Abstracts;
using TableDependency.Delegates;
using TableDependency.Enums;
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

        protected bool _databseObjectCreationConcluded;
        protected SqlServerVersion _sqlVersion = SqlServerVersion.Unknown;
        protected Guid _dialogHandle;

        protected const string DisposeMessageTemplate = "{0}/Dispose";
        protected const string StartMessageTemplate = "{0}/StartDialog/{1}";

        #endregion

        #region Properties

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

        /// <summary>
        /// Gets or sets the encoding use to convert database strings.
        /// </summary>
        public override Encoding Encoding { get; set; } = Encoding.Unicode;

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
        /// <param name="tableName">Name of the table.</param>
        /// <param name="mapper">The model to database table column mapper.</param>
        /// <param name="updateOf">List of columns that need to monitor for changing on order to receive notifications.</param>
        /// <param name="filter">The filter condition translated in WHERE.</param>
        /// <param name="notifyOn">The notify on Insert, Delete, Update operation.</param>
        /// <param name="teardown">if set to <c>true</c> drop all database object on stopping.</param>
        /// <param name="objectNaming">The object naming adopted (used to reconnect to not dropped queue).</param>
        public SqlTableDependency(
            string connectionString,
            string tableName = null,
            IModelToTableMapper<T> mapper = null,
            IUpdateOfModel<T> updateOf = null,
            ITableDependencyFilter filter = null,
            DmlTriggerType notifyOn = DmlTriggerType.All,
            bool teardown = true,
            string objectNaming = null) : base(connectionString, tableName, mapper, updateOf, filter, notifyOn, teardown, objectNaming)
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
                    _dialogHandle,
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

        #region Protected methods

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
                if (tableName.Contains("."))
                {
                    var splitted = tableName.Split('.');
                    return splitted[1].Replace("[", string.Empty).Replace("]", string.Empty);
                }

                return tableName.Replace("[", string.Empty).Replace("]", string.Empty);
            }

            return !string.IsNullOrWhiteSpace(GetTableNameFromTableDataAnnotation()) ? GetTableNameFromTableDataAnnotation() : typeof(T).Name;
        }

        protected override string GetSchemaName(string tableName)
        {
            // If no default schema is defined for a user account, SQL Server will assume dbo is the default schema. 
            // It is important note that if the user is authenticated by SQL Server via the Windows operating system, no default schema will be associated with the user. 
            // Therefore if the user creates an object, a new schema will be created and named the same as the user, 
            // and the object will be associated with that user schema, though not directly with the user.
            if (!string.IsNullOrWhiteSpace(tableName))
            {
                if (!tableName.Contains(".")) return "dbo";
                var splitted = tableName.Split('.');
                return splitted[0].Trim() != string.Empty ? splitted[0].Replace("[", string.Empty).Replace("]", string.Empty) : string.Empty;
            }

            return !string.IsNullOrWhiteSpace(GetSchemaNameFromTableDataAnnotation()) ? GetSchemaNameFromTableDataAnnotation() : "dbo";
        }

        protected int GetSchemaId(string schemaName, string connectionString)
        {
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"SELECT [schema_id] FROM [sys].[schemas] WHERE [name] = '{schemaName}'";
                    return (int)sqlCommand.ExecuteScalar();
                }
            }
        }

        protected SqlServerVersion GetSqlServerVersion(string connectionString)
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
                        var size = ComputeSize(
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

        protected IList<string> RetrieveProcessableMessages(IEnumerable<ColumnInfo> userInterestedColumns, string databaseObjectsNaming)
        {
            var processableMessages = new List<string>
            {
                string.Format(StartMessageTemplate, databaseObjectsNaming, ChangeType.Insert),
                string.Format(StartMessageTemplate, databaseObjectsNaming, ChangeType.Update),
                string.Format(StartMessageTemplate, databaseObjectsNaming, ChangeType.Delete),
                SqlMessageTypes.EndDialogType
            };

            processableMessages.AddRange(userInterestedColumns.Select(userInterestedColumn => $"{databaseObjectsNaming}/{userInterestedColumn.Name}"));

            return processableMessages;
        }

        protected override IList<string> CreateDatabaseObjects(string connectionString, string tableName, string dataBaseObjectsNamingConvention, IEnumerable<ColumnInfo> userInterestedColumns, IList<string> updateOf, int timeOut, int watchDogTimeOut)
        {
            var interestedColumns = userInterestedColumns as ColumnInfo[] ?? userInterestedColumns.ToArray();
            var columnsForTableVariable = PrepareColumnListForTableVariable(interestedColumns);
            var columnsForSelect = string.Join(",", interestedColumns.Select(c => $"[{c.Name}]").ToList());
            var columnsForUpdateOf = _updateOf != null ? string.Join(" OR ", _updateOf.Where(c => !string.IsNullOrWhiteSpace(c)).Distinct(StringComparer.CurrentCultureIgnoreCase).Select(c => $"UPDATE([{c}])").ToList()) : null;

            return this.CreateDatabaseObjects(connectionString, dataBaseObjectsNamingConvention, interestedColumns, columnsForTableVariable, columnsForSelect, columnsForUpdateOf, watchDogTimeOut);
        }

        protected override string GetBaseObjectsNamingConvention(string objectNaming)
        {
            if (!string.IsNullOrWhiteSpace(objectNaming)) return objectNaming;

            string name = $"{_schemaName}_{_tableName}";
            return $"{name}_{Guid.NewGuid()}";
        }

        protected override void DropDatabaseObjects(string connectionString, string databaseObjectsNaming)
        {
            if (_databseObjectCreationConcluded == false) return;

            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    var disposeMessage = string.Format(DisposeMessageTemplate, databaseObjectsNaming);

                    // Dialog timer messages are empty messages. 
                    // A receive operation receives the dialog timer message before any other message for that dialog, 
                    // regardless of the order in which the time-out message arrived on the queue.
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandText = string.Format(SqlScripts.DisposeMessage, databaseObjectsNaming, disposeMessage, this.SchemaName);
                    sqlCommand.ExecuteNonQuery();
                }
            }

            this.WriteTraceMessage(TraceLevel.Info, "DropDatabaseObjects ended.");
        }

        protected override void CheckRdbmsDependentImplementation(string connectionString)
        {            
            CheckIfServiceBrokerIsEnabled(connectionString);      
                 
            _sqlVersion = this.GetSqlServerVersion(connectionString);
            if (_sqlVersion == SqlServerVersion.SqlServer2000) throw new SqlServerVersionNotSupportedException(SqlServerVersion.SqlServer2000);
        }

        protected string CreateWhereCondifition(bool prependSpace = false)
        {
            var where = string.Empty;

            var filter = _filter?.Translate();
            if (!string.IsNullOrWhiteSpace(filter)) where = (prependSpace ? " " : string.Empty) + "WHERE " + filter;

            return where;
        }

        protected IList<string> CreateDatabaseObjects(string connectionString, string databaseObjectsNaming, IEnumerable<ColumnInfo> userInterestedColumns, string tableColumns, string selectColumns, string updateColumns, int watchDogTimeOut)
        {
            var processableMessages = new List<string>();

            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();

                using (var transaction = sqlConnection.BeginTransaction())
                {
                    var sqlCommand = new SqlCommand($"SELECT COUNT(*) FROM sys.service_queues WHERE name = N'{databaseObjectsNaming}'", sqlConnection, transaction);
                    if ((int)sqlCommand.ExecuteScalar() > 0) throw new DbObjectsWithSameNameException(databaseObjectsNaming);

                    var startMessageInsert = string.Format(StartMessageTemplate, databaseObjectsNaming, ChangeType.Insert);
                    sqlCommand.CommandText = $"CREATE MESSAGE TYPE [{startMessageInsert}] VALIDATION = NONE;";
                    sqlCommand.ExecuteNonQuery();
                    processableMessages.Add(startMessageInsert);

                    var startMessageUpdate = string.Format(StartMessageTemplate, databaseObjectsNaming, ChangeType.Update);
                    sqlCommand.CommandText = $"CREATE MESSAGE TYPE [{startMessageUpdate}] VALIDATION = NONE;";
                    sqlCommand.ExecuteNonQuery();
                    processableMessages.Add(startMessageUpdate);

                    var startMessageDelete = string.Format(StartMessageTemplate, databaseObjectsNaming, ChangeType.Delete);
                    sqlCommand.CommandText = $"CREATE MESSAGE TYPE [{startMessageDelete}] VALIDATION = NONE;";
                    sqlCommand.ExecuteNonQuery();
                    processableMessages.Add(startMessageDelete);

                    var disposeMessage = string.Format(DisposeMessageTemplate, databaseObjectsNaming);
                    sqlCommand.CommandText = $"CREATE MESSAGE TYPE [{disposeMessage}] VALIDATION = NONE;";
                    sqlCommand.ExecuteNonQuery();
                    processableMessages.Add(disposeMessage);

                    var interestedColumns = userInterestedColumns as ColumnInfo[] ?? userInterestedColumns.ToArray();
                    foreach (var userInterestedColumn in interestedColumns)
                    {
                        var message = $"{databaseObjectsNaming}/{userInterestedColumn.Name}";
                        sqlCommand.CommandText = $"CREATE MESSAGE TYPE [{message}] VALIDATION = NONE;";
                        sqlCommand.ExecuteNonQuery();
                        processableMessages.Add(message);
                    }
                    this.WriteTraceMessage(TraceLevel.Verbose, "Message types created.");

                    var contractBody = string.Join("," + Environment.NewLine, processableMessages.Select(message => $"[{message}] SENT BY INITIATOR"));
                    sqlCommand.CommandText = $"CREATE CONTRACT [{databaseObjectsNaming}] ({contractBody})";
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, "Contract created.");

                    var dropMessages = string.Join(Environment.NewLine, processableMessages.Select(c => string.Format("IF EXISTS (SELECT * FROM sys.service_message_types WHERE name = N'{0}') DROP MESSAGE TYPE[{0}];", c)));
                    var dropAllScript = string.Format(SqlScripts.ScriptDropAll, databaseObjectsNaming, dropMessages, _schemaName, _tableName);
                    sqlCommand.CommandText = string.Format(SqlScripts.CreateProcedureQueueActivation, databaseObjectsNaming, dropAllScript, _schemaName, disposeMessage);
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, "Procedure Queue Activation created.");

                    sqlCommand.CommandText = _sqlVersion == SqlServerVersion.SqlServer2005
                        ? $"CREATE QUEUE {_schemaName}.[{databaseObjectsNaming}] WITH STATUS = ON, RETENTION = OFF, ACTIVATION (PROCEDURE_NAME = {_schemaName}.[{databaseObjectsNaming}_QueueActivation], MAX_QUEUE_READERS = 1, EXECUTE AS {this.QueueExecuteAs.ToUpper()})"
                        : $"CREATE QUEUE {_schemaName}.[{databaseObjectsNaming}] WITH STATUS = ON, RETENTION = OFF, POISON_MESSAGE_HANDLING (STATUS = OFF), ACTIVATION (PROCEDURE_NAME = {_schemaName}.[{databaseObjectsNaming}_QueueActivation], MAX_QUEUE_READERS = 1, EXECUTE AS {this.QueueExecuteAs.ToUpper()})";
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, "Queue created.");

                    sqlCommand.CommandText = string.IsNullOrWhiteSpace(this.ServiceAuthorization)
                        ? $"CREATE SERVICE [{databaseObjectsNaming}] ON QUEUE {_schemaName}.[{databaseObjectsNaming}] ([{databaseObjectsNaming}])"
                        : $"CREATE SERVICE [{databaseObjectsNaming}] AUTHORIZATION [{this.ServiceAuthorization}] ON QUEUE {_schemaName}.[{databaseObjectsNaming}] ([{databaseObjectsNaming}])";
                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, "Service created.");

                    var declareVariableStatement = PrepareDeclareVariableStatement(interestedColumns);
                    var selectForSetVariablesStatement = PrepareSelectForSetVariables(interestedColumns);
                    var sendInsertConversationStatements = PrepareSendConversation(databaseObjectsNaming, ChangeType.Insert, interestedColumns);
                    var sendUpdatedConversationStatements = PrepareSendConversation(databaseObjectsNaming, ChangeType.Update, interestedColumns);
                    var sendDeletedConversationStatements = PrepareSendConversation(databaseObjectsNaming, ChangeType.Delete, interestedColumns);
                    var exceptStatement = PrepareExceptStatement(interestedColumns);
                    var bodyForUpdate = !string.IsNullOrEmpty(updateColumns)
                        ? string.Format(SqlScripts.TriggerUpdateWithColumns, updateColumns, _tableName, selectColumns, ChangeType.Update, exceptStatement)
                        : string.Format(SqlScripts.TriggerUpdateWithoutColumns, _tableName, selectColumns, ChangeType.Update, exceptStatement);

                    sqlCommand.CommandText = string.Format(
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
                        string.Join(",", GetDmlTriggerType(_dmlTriggerType)),
                        CreateWhereCondifition());

                    sqlCommand.ExecuteNonQuery();
                    this.WriteTraceMessage(TraceLevel.Verbose, "Trigger created.");

                    var sqlParameter = new SqlParameter { ParameterName = "@handle", DbType = DbType.Guid, Direction = ParameterDirection.Output };
                    sqlCommand.CommandText = string.Format("begin dialog conversation @handle from service [{0}] to service '{0}', 'CURRENT DATABASE' on contract [{0}] with encryption = off;", databaseObjectsNaming);
                    sqlCommand.Parameters.Add(sqlParameter);
                    sqlCommand.ExecuteNonQuery();
                    _dialogHandle = (Guid)sqlParameter.Value;

                    sqlCommand.CommandText = $"begin conversation timer ('{_dialogHandle}') timeout = {watchDogTimeOut};";
                    sqlCommand.ExecuteNonQuery();

                    transaction.Commit();                    

                    processableMessages.Add(SqlMessageTypes.EndDialogType);

                    _databseObjectCreationConcluded = true;
                }
            }

            this.WriteTraceMessage(TraceLevel.Info, $"Database objects created with naming {databaseObjectsNaming}.");            

            return processableMessages;
        }

        protected string PrepareExceptStatement(IReadOnlyCollection<ColumnInfo> interestedColumns)
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

        protected static IEnumerable<string> GetDmlTriggerType(DmlTriggerType dmlTriggerType)
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

        protected static MessagesBag CreateMessagesBag(string databaseObjectsNaming, Encoding encoding)
        {
            return new MessagesBag(
                encoding ?? Encoding.Unicode,
                new List<string>
                {
                    string.Format(StartMessageTemplate, databaseObjectsNaming, ChangeType.Insert),
                    string.Format(StartMessageTemplate, databaseObjectsNaming, ChangeType.Update),
                    string.Format(StartMessageTemplate, databaseObjectsNaming, ChangeType.Delete)
                },
                SqlMessageTypes.EndDialogType);
        }

        protected static void EndConversation(SqlConnection sqlConnection, SqlGuid handle)
        {
            using (var sqlCommand = sqlConnection.CreateCommand())
            {
                sqlCommand.CommandText = "end conversation @handle";
                sqlCommand.Parameters.Add("@handle", SqlDbType.UniqueIdentifier);
                sqlCommand.Parameters["@handle"].Value = handle;
                sqlCommand.ExecuteNonQuery();
            }
        }

        protected static string PrepareColumnListForTableVariable(IEnumerable<ColumnInfo> tableColumns)
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

        protected static void ThrowIfSqlClientCancellationRequested(CancellationToken cancellationToken, Exception exception)
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

        protected void NotifyListenersAboutChange(Delegate[] changeSubscribedList, IModelToTableMapper<T> modelMapper, MessagesBag messagesBag, IEnumerable<ColumnInfo> userInterestedColumns)
        {
            if (changeSubscribedList == null) return;

            foreach (var dlg in changeSubscribedList.Where(d => d != null))
            {
                dlg.GetMethodInfo().Invoke(dlg.Target, new object[] { null, new SqlRecordChangedEventArgs<T>(
                    messagesBag,
                    modelMapper,
                    userInterestedColumns,
                    _server,
                    _database,
                    _dataBaseObjectsNamingConvention) });
            }
        }

        protected static string ComputeSize(string dataType, string characterMaximumLength, string numericPrecision, string numericScale, string dateTimePrecisione)
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

        protected static string ConvertFormat(ColumnInfo userInterestedColumn)
        {
            return string.Equals(userInterestedColumn.Type, "datetime", StringComparison.OrdinalIgnoreCase) || string.Equals(userInterestedColumn.Type, "date", StringComparison.OrdinalIgnoreCase) ? ", 121" : string.Empty;
        }

        protected static string ConvertValueByType(IReadOnlyCollection<ColumnInfo> userInterestedColumns, ColumnInfo userInterestedColumn)
        {
            if (string.Equals(userInterestedColumn.Type, "binary", StringComparison.OrdinalIgnoreCase) || string.Equals(userInterestedColumn.Type, "varbinary", StringComparison.OrdinalIgnoreCase) || string.Equals(userInterestedColumn.Type, "timestamp", StringComparison.OrdinalIgnoreCase))
            {
                return SanitizeVariableName(userInterestedColumns, userInterestedColumn.Name);
            }

            return $"convert(nvarchar(max), {SanitizeVariableName(userInterestedColumns, userInterestedColumn.Name)}{ConvertFormat(userInterestedColumn)})";
        }

        protected static string PrepareSendConversation(string databaseObjectsNaming, ChangeType dmlType, IReadOnlyCollection<ColumnInfo> userInterestedColumns)
        {
            var sendList = userInterestedColumns
                .Select(insterestedColumn => $"IF {SanitizeVariableName(userInterestedColumns, insterestedColumn.Name)} IS NOT NULL BEGIN" + Environment.NewLine + $";send on conversation @h message type[{databaseObjectsNaming}/{insterestedColumn.Name}] ({ConvertValueByType(userInterestedColumns, insterestedColumn)})" + Environment.NewLine + "END" + Environment.NewLine + "ELSE BEGIN" + Environment.NewLine + $";send on conversation @h message type[{databaseObjectsNaming}/{insterestedColumn.Name}] (0x)" + Environment.NewLine + "END")
                .ToList();

            sendList.Insert(0, $";send on conversation @h message type[{string.Format(StartMessageTemplate, databaseObjectsNaming, dmlType)}] (convert(nvarchar, @dmlType))" + Environment.NewLine);

            return string.Join(Environment.NewLine, sendList);
        }

        protected static string PrepareSelectForSetVariables(IReadOnlyCollection<ColumnInfo> userInterestedColumns)
        {
            return string.Join(",", userInterestedColumns.Select(insterestedColumn => $"{SanitizeVariableName(userInterestedColumns, insterestedColumn.Name)} = [{insterestedColumn.Name}]"));
        }

        protected static string PrepareDeclareVariableStatement(IReadOnlyCollection<ColumnInfo> interestedColumns)
        {
            var colonne = (from insterestedColumn in interestedColumns
                           let variableType = $"{insterestedColumn.Type.ToLowerInvariant()}" + (string.IsNullOrWhiteSpace(insterestedColumn.Size)
                           ? string.Empty
                           : $"({insterestedColumn.Size})")
                           select $"DECLARE {SanitizeVariableName(interestedColumns, insterestedColumn.Name)} {variableType.ToLowerInvariant()}").ToList();

            return string.Join(Environment.NewLine, colonne);
        }

        protected static string SanitizeVariableName(IReadOnlyCollection<ColumnInfo> userInterestedColumns, string tableColumnName)
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

        protected static void CheckIfServiceBrokerIsEnabled(string connectionString)
        {
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = "SELECT is_broker_enabled FROM sys.databases WHERE database_id = db_id()";
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

        protected async Task WaitForNotifications(
            CancellationToken cancellationToken,
            Delegate[] onChangeSubscribedList,
            Delegate[] onErrorSubscribedList,
            Delegate[] onStatusChangedSubscribedList,
            Guid dialogHandle,
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

            var waitforSqlScript = $"begin conversation timer ('{dialogHandle}') timeout = {timeOutWatchDog}; WAITFOR(receive top ({processableMessages.Count}) [conversation_handle], [message_type_name], [message_body] FROM {schemaName}.[{databaseObjectsNaming}]), timeout {timeOut * 1000};";
            var newMessageReadyToBeNotified = false;

            try
            {
                NotifyListenersAboutStatus(onStatusChangedSubscribedList, TableDependencyStatus.Started);

                while (true)
                {
                    var messagesBag = CreateMessagesBag(databaseObjectsNaming, encoding);

                    try
                    {
                        using (var sqlConnection = new SqlConnection(connectionString))
                        {
                            await sqlConnection.OpenAsync(cancellationToken);
                            this.WriteTraceMessage(TraceLevel.Verbose, "Connection opened.");

                            using (var sqlCommand = sqlConnection.CreateCommand())
                            {
                                sqlCommand.CommandText = waitforSqlScript;
                                sqlCommand.CommandTimeout = 0;

                                NotifyListenersAboutStatus(onStatusChangedSubscribedList, TableDependencyStatus.WaitingForNotification);
                                this.WriteTraceMessage(TraceLevel.Verbose, "Running WAITFOR command.");

                                using (var sqlDataReader = await sqlCommand.ExecuteReaderAsync(cancellationToken).WithCancellation(cancellationToken))
                                {
                                    while (sqlDataReader.Read())
                                    {
                                        var messageType = sqlDataReader.IsDBNull(1) ? null : sqlDataReader.GetSqlString(1);
                                        this.WriteTraceMessage(TraceLevel.Verbose, $"DB message received. Message type = {messageType}.");

                                        if (messageType.Value == SqlMessageTypes.ErrorType)
                                        {
                                            this.WriteTraceMessage(TraceLevel.Verbose, $"Invalid message type [{messageType.Value}].");
                                            if (messageType.Value == SqlMessageTypes.ErrorType) throw new ServiceBrokerErrorMessageException(databaseObjectsNaming);
                                            throw new ServiceBrokerEndDialogException(databaseObjectsNaming);
                                        }

                                        if (processableMessages.Contains(messageType.Value))
                                        {
                                            var messageContent = sqlDataReader.IsDBNull(2) ? null : sqlDataReader.GetSqlBytes(2).Value;
                                            var messageStatus = messagesBag.AddMessage(messageType.Value, messageContent);
                                            if (messageStatus == MessagesBagStatus.Closed)
                                            {
                                                newMessageReadyToBeNotified = true;
                                                NotifyListenersAboutStatus(onStatusChangedSubscribedList, TableDependencyStatus.MessageReadyToBeNotified);
                                                this.WriteTraceMessage(TraceLevel.Verbose, "Message ready to be notified.");
                                                break;
                                            }
                                        }
                                        else
                                        {
                                            this.WriteTraceMessage(TraceLevel.Warning, $"Message discarted [{messageType}].");
                                        }
                                    }
                                }
                            }
                        }

                        if (newMessageReadyToBeNotified)
                        {
                            newMessageReadyToBeNotified = false;

                            NotifyListenersAboutChange(onChangeSubscribedList, modelMapper, messagesBag, userInterestedColumns);
                            this.WriteTraceMessage(TraceLevel.Verbose, "Message notified.");

                            NotifyListenersAboutStatus(onStatusChangedSubscribedList, TableDependencyStatus.MessageSent);
                        }
                    }
                    catch (Exception exception)
                    {
                        ThrowIfSqlClientCancellationRequested(cancellationToken, exception);
                        throw;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                NotifyListenersAboutStatus(onStatusChangedSubscribedList, TableDependencyStatus.StoppedDueToCancellation);
                this.WriteTraceMessage(TraceLevel.Info, "Operation canceled.");
            }
            catch (Exception exception)
            {
                NotifyListenersAboutStatus(onStatusChangedSubscribedList, TableDependencyStatus.StoppedDueToError);
                if (cancellationToken.IsCancellationRequested == false) NotifyListenersAboutError(onErrorSubscribedList, exception);
                this.WriteTraceMessage(TraceLevel.Error, "Exception in WaitForNotifications.", exception);
            }
            finally
            {
                if (dialogHandle != Guid.Empty)
                {
                    using (var sqlConnection = new SqlConnection(connectionString))
                    {
                        EndConversation(sqlConnection, dialogHandle);
                    }
                }
            }

            this.WriteTraceMessage(TraceLevel.Verbose, "Exiting from WaitForNotifications.");
        }

        #endregion
    }
}