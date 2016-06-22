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
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Oracle.ManagedDataAccess.Client;
using TableDependency.Classes;
using TableDependency.Delegates;
using TableDependency.Enums;
using TableDependency.Exceptions;
using TableDependency.Extensions;
using TableDependency.Mappers;
using TableDependency.Messages;
using TableDependency.OracleClient.Enumerations;
using TableDependency.OracleClient.EventArgs;
using TableDependency.OracleClient.Exceptions;
using TableDependency.OracleClient.Helpers;
using TableDependency.OracleClient.Resources;
using TableDependency.Utilities;

namespace TableDependency.OracleClient
{
    /// <summary>
    /// TableDependency implementation for Oracle.
    /// </summary>
    public class OracleTableDependency<T> : TableDependency<T> where T : class
    {
        #region Private variables

        private const string MyName = "OracleTableDependency";
        private const string Quotes = "\"";

        #endregion

        #region Properties

        public override Encoding Encoding { get; set; } = Encoding.UTF8;

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
        /// Initializes a new instance of the <see cref="OracleTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public OracleTableDependency(string connectionString)
            : base(MyName, connectionString, null, null, (IList<string>)null, DmlTriggerType.All, true)
        {
            _tableName = _tableName.ToUpper();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OracleTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        public OracleTableDependency(string connectionString, string tableName)
            : base(MyName, connectionString, tableName, null, (IList<string>)null, DmlTriggerType.All, true, null)
        {
            _tableName = _tableName.ToUpper();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OracleTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="mapper">Model to columns table mapper.</param>
        public OracleTableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper)
            : base(MyName, connectionString, tableName, mapper, (IList<string>)null, DmlTriggerType.All, true, null)
        {
            _tableName = _tableName.ToUpper();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OracleTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        public OracleTableDependency(string connectionString, string tableName, IList<string> updateOf)
            : base(MyName, connectionString, tableName, null, updateOf, DmlTriggerType.All, true, null)
        {
            _tableName = _tableName.ToUpper();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OracleTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="mapper">Model to columns table mapper.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        public OracleTableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper, IList<string> updateOf)
            : base(MyName, connectionString, tableName, mapper, updateOf, DmlTriggerType.All, true, null)
        {
            _tableName = _tableName.ToUpper();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OracleTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="mapper">Model to columns table mapper.</param>
        /// <param name="automaticDatabaseObjectsTeardown">Destroy all database objects created for receive notifications.</param>
        /// <param name="namingConventionForDatabaseObjects">The naming convention for database objects.</param>
        public OracleTableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
            : base(MyName, connectionString, tableName, mapper, (IList<string>)null, DmlTriggerType.All, automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects)
        {
            _tableName = _tableName.ToUpper();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OracleTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="mapper">Model to columns table mapper.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        /// <param name="automaticDatabaseObjectsTeardown">Destroy all database objects created for receive notifications.</param>
        /// <param name="namingConventionForDatabaseObjects">The naming convention for database objects.</param>
        public OracleTableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper, IList<string> updateOf, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
            : base(MyName, connectionString, tableName, mapper, updateOf, DmlTriggerType.All, automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects)
        {
            _tableName = _tableName.ToUpper();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OracleTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        /// <param name="automaticDatabaseObjectsTeardown">Destroy all database objects created for receive notifications.</param>
        /// <param name="namingConventionForDatabaseObjects">The naming convention for database objects.</param>
        public OracleTableDependency(string connectionString, string tableName, IList<string> updateOf, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
            : base(MyName, connectionString, tableName, null, updateOf, DmlTriggerType.All, automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects)
        {
            _tableName = _tableName.ToUpper();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OracleTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        /// <param name="automaticDatabaseObjectsTeardown">Destroy all database objects created for receive notifications.</param>
        /// <param name="namingConventionForDatabaseObjects">The naming convention for database objects.</param>
        public OracleTableDependency(string connectionString, IList<string> updateOf, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
            : base(MyName, connectionString, null, null, updateOf, DmlTriggerType.All, automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects)
        {
            _tableName = _tableName.ToUpper();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OracleTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        public OracleTableDependency(string connectionString, IList<string> updateOf)
            : base(MyName, connectionString, null, null, updateOf, DmlTriggerType.All, true, null)
        {
            _tableName = _tableName.ToUpper();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OracleTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        public OracleTableDependency(string connectionString, string tableName, UpdateOfModel<T> updateOf)
            : base(MyName, connectionString, tableName, null, updateOf, DmlTriggerType.All, true, null)
        {
            _tableName = _tableName.ToUpper();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OracleTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="mapper">Model to columns table mapper.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        public OracleTableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper, UpdateOfModel<T> updateOf)
            : base(MyName, connectionString, tableName, mapper, updateOf, DmlTriggerType.All, true, null)
        {
            _tableName = _tableName.ToUpper();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OracleTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="mapper">Model to columns table mapper.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        /// <param name="automaticDatabaseObjectsTeardown">Destroy all database objects created for receive notifications.</param>
        /// <param name="namingConventionForDatabaseObjects">The naming convention for database objects.</param>
        public OracleTableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper, UpdateOfModel<T> updateOf, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
            : base(MyName, connectionString, tableName, mapper, updateOf, DmlTriggerType.All, automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects)
        {
            _tableName = _tableName.ToUpper();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OracleTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="mapper">Model to columns table mapper.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        /// <param name="dmlTriggerType">Type of the DML trigger.</param>
        /// <param name="automaticDatabaseObjectsTeardown">Destroy all database objects created for receive notifications.</param>
        /// <param name="namingConventionForDatabaseObjects">The naming convention for database objects.</param>
        public OracleTableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper, UpdateOfModel<T> updateOf, DmlTriggerType dmlTriggerType, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
            : base(MyName, connectionString, tableName, mapper, updateOf, dmlTriggerType, automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects)
        {
            _tableName = _tableName.ToUpper();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OracleTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        /// <param name="automaticDatabaseObjectsTeardown">Destroy all database objects created for receive notifications.</param>
        /// <param name="namingConventionForDatabaseObjects">The naming convention for database objects.</param>
        public OracleTableDependency(string connectionString, string tableName, UpdateOfModel<T> updateOf, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
            : base(MyName, connectionString, tableName, null, updateOf, DmlTriggerType.All, automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects)
        {
            _tableName = _tableName.ToUpper();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OracleTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        /// <param name="dmlTriggerType">Type of the DML trigger.</param>
        /// <param name="automaticDatabaseObjectsTeardown">Destroy all database objects created for receive notifications.</param>
        /// <param name="namingConventionForDatabaseObjects">The naming convention for database objects.</param>
        public OracleTableDependency(string connectionString, string tableName, UpdateOfModel<T> updateOf, DmlTriggerType dmlTriggerType, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
            : base(MyName, connectionString, tableName, null, updateOf, dmlTriggerType, automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects)
        {
            _tableName = _tableName.ToUpper();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OracleTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        /// <param name="automaticDatabaseObjectsTeardown">Destroy all database objects created for receive notifications.</param>
        /// <param name="namingConventionForDatabaseObjects">The naming convention for database objects.</param>
        public OracleTableDependency(string connectionString, UpdateOfModel<T> updateOf, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
            : base(MyName, connectionString, null, null, updateOf, DmlTriggerType.All, automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects)
        {
            _tableName = _tableName.ToUpper();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OracleTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        public OracleTableDependency(string connectionString, UpdateOfModel<T> updateOf)
            : base(MyName, connectionString, null, null, updateOf, DmlTriggerType.All, true, null)
        {
            _tableName = _tableName.ToUpper();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OracleTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="mapper">The mapper.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        /// <param name="dmlTriggerType">Type of the DML trigger.</param>
        /// <param name="automaticDatabaseObjectsTeardown">Destroy all database objects created for receive notifications.</param>
        /// <param name="namingConventionForDatabaseObjects">The naming convention for database objects.</param>
        public OracleTableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper, IList<string> updateOf, DmlTriggerType dmlTriggerType, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
            : base(MyName, connectionString, tableName, mapper, updateOf, dmlTriggerType, automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects)
        {
            _tableName = _tableName.ToUpper();
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
        public override void Start(int timeOut = 120, int watchDogTimeOut = 180)
        {
            if (OnChanged == null) throw new NoSubscriberException();

            base.Start(timeOut, watchDogTimeOut);

            var onChangedSubscribedList = OnChanged.GetInvocationList();
            var onErrorSubscribedList = OnError?.GetInvocationList();
            var onStatusChangedSubscribedList = OnStatusChanged?.GetInvocationList();
            NotifyListenersAboutStatus(onStatusChangedSubscribedList, TableDependencyStatus.Starting);

            _cancellationTokenSource = new CancellationTokenSource();
            _task = Task.Factory.StartNew(() =>
                WaitForNotifications(
                    _cancellationTokenSource.Token,
                    onChangedSubscribedList,
                    onErrorSubscribedList,
                    onStatusChangedSubscribedList,
                    _connectionString,
                    _dataBaseObjectsNamingConvention,
                    watchDogTimeOut,
                    _mapper,
                    _processableMessages,
                    _automaticDatabaseObjectsTeardown,
                    _userInterestedColumns,
                    this.Encoding),
                _cancellationTokenSource.Token);

            this.WriteTraceMessage(TraceLevel.Info, "Started waiting for notification.");
        }

        #endregion

        #region Protected methods

        protected override IList<string> RetrieveProcessableMessages(IEnumerable<ColumnInfo> columnsTableList, string databaseObjectsNaming)
        {
            var insertMessageTypes = columnsTableList.Select(c => $"{databaseObjectsNaming}/{ChangeType.Insert}/{c.Name.Replace(Quotes, string.Empty)}").ToList();
            var updateMessageTypes = columnsTableList.Select(c => $"{databaseObjectsNaming}/{ChangeType.Update}/{c.Name.Replace(Quotes, string.Empty)}").ToList();
            var deleteMessageTypes = columnsTableList.Select(c => $"{databaseObjectsNaming}/{ChangeType.Delete}/{c.Name.Replace(Quotes, string.Empty)}").ToList();
            var messageBoundaries = new List<string> { string.Format(StartMessageTemplate, databaseObjectsNaming), string.Format(EndMessageTemplate, databaseObjectsNaming) };

            return insertMessageTypes.Concat(updateMessageTypes).Concat(deleteMessageTypes).Concat(messageBoundaries).ToList();
        }

        protected override IList<string> CreateDatabaseObjects(string connectionString, string tableName, string dataBaseObjectsNamingConvention, IEnumerable<ColumnInfo> userInterestedColumns, IList<string> updateOf, int timeOut, int timeOutWatchDog)
        {
            try
            {
                using (var connection = new OracleConnection(connectionString))
                {
                    connection.Open();

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = $"CREATE TYPE TYPE_{dataBaseObjectsNamingConvention} AS OBJECT(message_type VARCHAR2(100), message BLOB);";
                        command.ExecuteNonQuery();
                        command.CommandText = $"CREATE TYPE TBL_{dataBaseObjectsNamingConvention} IS TABLE OF TYPE_{dataBaseObjectsNamingConvention};";
                        command.ExecuteNonQuery();

                        command.CommandText = $"BEGIN DBMS_AQADM.CREATE_QUEUE_TABLE(queue_table=> 'QT_{dataBaseObjectsNamingConvention}', queue_payload_type=> 'TYPE_{dataBaseObjectsNamingConvention}', multiple_consumers => FALSE); END;";
                        command.ExecuteNonQuery();
                        command.CommandText = $"BEGIN DBMS_AQADM.CREATE_QUEUE(queue_name => 'QUE_{dataBaseObjectsNamingConvention}', queue_table => 'QT_{dataBaseObjectsNamingConvention}'); END;";
                        command.ExecuteNonQuery();
                        command.CommandText = $"BEGIN DBMS_AQADM.START_QUEUE(queue_name=> 'QUE_{dataBaseObjectsNamingConvention}'); END;";
                        command.ExecuteNonQuery();

                        var declareStatement = string.Join(Environment.NewLine, userInterestedColumns.Select(c => "v_" + c.Name.Replace(" ", "_").Replace(Quotes, string.Empty) + " " + c.Type + c.Size + ";"));
                        var startMessageStatement = string.Format(StartMessageTemplate, dataBaseObjectsNamingConvention);
                        var endMessageStatement = string.Format(EndMessageTemplate, dataBaseObjectsNamingConvention);
                        var setNewValueStatement = string.Join(Environment.NewLine, userInterestedColumns.Select(c => "v_" + c.Name.Replace(" ", "_").Replace(Quotes, string.Empty) + " := :NEW." + c.Name + ";"));
                        var setOldValueStatement = string.Join(Environment.NewLine, userInterestedColumns.Select(c => "v_" + c.Name.Replace(" ", "_").Replace(Quotes, string.Empty) + " := :OLD." + c.Name + ";"));
                        var insertDml = ChangeType.Insert.ToString();
                        var updateDml = ChangeType.Update.ToString();
                        var deleteDml = ChangeType.Delete.ToString();

                        var enqueueStartMessage = PrepareStartEnqueueScript(dataBaseObjectsNamingConvention) + Environment.NewLine;
                        var enqueueFieldsStatement = string.Join(Environment.NewLine, userInterestedColumns.Select(c => this.PrepareEnqueueScript(c, dataBaseObjectsNamingConvention))) + Environment.NewLine;
                        var enqueueEndMessage = PrepareEndEnqueueScript(dataBaseObjectsNamingConvention);

                        var triggerOnlyValueChangeCondition = this.PrepareEventOnlyWhenAnyValueChangedCondition(userInterestedColumns);

                        command.CommandText = string.Format(
                            Scripts.CreateTriggerEnqueueMessage,
                            dataBaseObjectsNamingConvention,
                            GetUpdateOfStatement(userInterestedColumns, updateOf),
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
                            enqueueStartMessage + enqueueFieldsStatement + enqueueEndMessage,
                            string.Join(" OR ", GetDmlTriggerType(_dmlTriggerType)),
                            triggerOnlyValueChangeCondition);
                        command.ExecuteNonQuery();

                        command.CommandText = string.Format(Scripts.CreateProcedureDequeueMessage, dataBaseObjectsNamingConvention, timeOut, userInterestedColumns.Count() + 2);
                        command.ExecuteNonQuery();

                        command.CommandText = string.Format(Scripts.ScriptJobCreate, dataBaseObjectsNamingConvention, (timeOutWatchDog / 60), string.Format(Scripts.ScriptDropAll, dataBaseObjectsNamingConvention).Replace("'", "''"));
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch
            {
                DropDatabaseObjects(connectionString, dataBaseObjectsNamingConvention);
                throw;
            }

            this.WriteTraceMessage(TraceLevel.Info, $"Database objects created with naming {dataBaseObjectsNamingConvention}.");

            return RetrieveProcessableMessages(userInterestedColumns, dataBaseObjectsNamingConvention);
        }

        private string PrepareEventOnlyWhenAnyValueChangedCondition(IEnumerable<ColumnInfo> userInterestedColumns)
        {
            var conditions = new List<string>();

            foreach (var userInterestedColumn in userInterestedColumns)
            {
                if (userInterestedColumn.Type.ToUpper() == "XMLTYPE")
                {
                    conditions.Add("DBMS_LOB.COMPARE(:OLD." + userInterestedColumn.Name + ".GETCLOBVAL(), :NEW." + userInterestedColumn.Name + ".GETCLOBVAL()) = 0");
                }
                else
                {
                    conditions.Add(":OLD." + userInterestedColumn.Name + " = :NEW." + userInterestedColumn.Name);
                }
            }

            return string.Join(" AND ", conditions);
        }

        protected override IEnumerable<ColumnInfo> GetUserInterestedColumns(IEnumerable<string> updateOf)
        {
            var tableColumns = GetTableColumnsList(_connectionString, _tableName);
            if (!tableColumns.Any()) throw new NoColumnsException(_tableName);

            CheckUpdateOfValidity(tableColumns, updateOf);
            CheckMapperValidity(tableColumns);

            var userIterestedColumns = PrivateGetUserInterestedColumns(tableColumns);

            var columnsToUseForCreatingDbObjects = userIterestedColumns as ColumnInfo[] ?? userIterestedColumns.ToArray();
            CheckIfUserInterestedColumnsCanBeManaged(userIterestedColumns);
            return columnsToUseForCreatingDbObjects;
        }

        protected override string GeneratedataBaseObjectsNamingConvention(string namingConventionForDatabaseObjects)
        {
            if (string.IsNullOrWhiteSpace(namingConventionForDatabaseObjects))
            {
                return Get24DigitsGuid();
            }

            if (namingConventionForDatabaseObjects.Length > 25)
            {
                throw new TableDependencyException("Naming convention cannot be greater that 25 characters");
            }

            return namingConventionForDatabaseObjects;
        }

        protected override bool CheckIfNeedsToCreateDatabaseObjects()
        {
            var allObjectAlreadyPresent = new Dictionary<string, bool>();

            using (var connection = new OracleConnection(_connectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    var outParameter = command.Parameters.Add(new OracleParameter { ParameterName = "exist", OracleDbType = OracleDbType.Int32, Direction = ParameterDirection.Output });

                    command.CommandText = $"BEGIN SELECT COUNT(*) INTO :exist FROM USER_OBJECTS WHERE OBJECT_TYPE = 'TRIGGER' AND UPPER(OBJECT_NAME) = 'TR_{_dataBaseObjectsNamingConvention}'; END;";
                    command.ExecuteNonQuery();
                    allObjectAlreadyPresent.Add($"TRIGGER with name 'TR_{_dataBaseObjectsNamingConvention}'", int.Parse(outParameter.Value.ToString()) > 0);

                    command.CommandText = $"BEGIN SELECT COUNT(*) INTO :exist FROM USER_OBJECTS WHERE OBJECT_TYPE = 'PROCEDURE' AND UPPER(OBJECT_NAME) = 'DEQ_{_dataBaseObjectsNamingConvention}'; END;";
                    command.ExecuteNonQuery();
                    allObjectAlreadyPresent.Add($"PROCEDURE with name 'DEQ_{_dataBaseObjectsNamingConvention}'", int.Parse(outParameter.Value.ToString()) > 0);

                    command.CommandText = $"BEGIN SELECT COUNT(*) INTO :exist FROM USER_OBJECTS WHERE OBJECT_TYPE = 'QUEUE' AND UPPER(OBJECT_NAME) = 'QUE_{_dataBaseObjectsNamingConvention}'; END;";
                    command.ExecuteNonQuery();
                    allObjectAlreadyPresent.Add($"QUEUE with name 'QUE_{_dataBaseObjectsNamingConvention}'", int.Parse(outParameter.Value.ToString()) > 0);

                    command.CommandText = $"BEGIN SELECT COUNT(*) INTO :exist FROM USER_OBJECTS WHERE OBJECT_TYPE = 'TABLE' AND UPPER(OBJECT_NAME) = 'QT_{_dataBaseObjectsNamingConvention}'; END;";
                    command.ExecuteNonQuery();
                    allObjectAlreadyPresent.Add($"TABLE QUEUE with name 'QT_{_dataBaseObjectsNamingConvention}'", int.Parse(outParameter.Value.ToString()) > 0);

                    command.CommandText = $"BEGIN SELECT COUNT(*) INTO :exist FROM USER_OBJECTS WHERE OBJECT_TYPE = 'TYPE' AND UPPER(OBJECT_NAME) = 'TBL_{_dataBaseObjectsNamingConvention}'; END;";
                    command.ExecuteNonQuery();
                    allObjectAlreadyPresent.Add($"TABLE TYPE with name 'TBL_{_dataBaseObjectsNamingConvention}'", int.Parse(outParameter.Value.ToString()) > 0);

                    command.CommandText = $"BEGIN SELECT COUNT(*) INTO :exist FROM USER_OBJECTS WHERE OBJECT_TYPE = 'TYPE' AND UPPER(OBJECT_NAME) = 'TYPE_{_dataBaseObjectsNamingConvention}'; END;";
                    command.ExecuteNonQuery();
                    allObjectAlreadyPresent.Add($"TYPE with name 'TYPE_{_dataBaseObjectsNamingConvention}'", int.Parse(outParameter.Value.ToString()) > 0);
                }
            }

            if (allObjectAlreadyPresent.All(exist => !exist.Value)) return true;
            if (allObjectAlreadyPresent.All(exist => exist.Value)) return false;

            // Not all objects are present
            throw new SomeDatabaseObjectsNotPresentException(allObjectAlreadyPresent);
        }

        protected override void DropDatabaseObjects(string connectionString, string databaseObjectsNaming)
        {
            using (var connection = new OracleConnection(connectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = string.Format("DECLARE counter INT; BEGIN SELECT COUNT(*) INTO counter FROM user_scheduler_jobs WHERE JOB_NAME = 'JOB_{0}'; DBMS_SCHEDULER.DROP_JOB('JOB_{0}', TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;", databaseObjectsNaming);
                    command.ExecuteNonQuery();

                    command.CommandType = CommandType.Text;
                    command.CommandText = string.Format(Scripts.ScriptDropAll, databaseObjectsNaming);
                    command.ExecuteNonQuery();
                }
            }

            this.WriteTraceMessage(TraceLevel.Info, "Database objects destroyed.");
        }

        protected override void PreliminaryChecks(string connectionString, string tableName)
        {
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

                CheckIfUserHasPermission(connection);
                CheckIfTableExists(connection, tableName);
            }
        }

        #endregion

        #region Private methods

        private static string PrepareStartEnqueueScript(string dataBaseObjectsNamingConvention)
        {
            return
                "SELECT UTL_RAW.CAST_TO_RAW(dmlType) INTO message_buffer FROM DUAL;" + Environment.NewLine +
                $"DBMS_AQ.ENQUEUE(queue_name => 'QUE_{dataBaseObjectsNamingConvention}', enqueue_options => enqueue_options, message_properties => message_properties, payload => TYPE_{dataBaseObjectsNamingConvention}(messageStart, message_buffer), msgid => message_handle);" + Environment.NewLine;
        }

        private static string PrepareEndEnqueueScript(string dataBaseObjectsNamingConvention)
        {
            return
                "SELECT UTL_RAW.CAST_TO_RAW(dmlType) INTO message_buffer FROM DUAL;" + Environment.NewLine +
                $"DBMS_AQ.ENQUEUE(queue_name => 'QUE_{dataBaseObjectsNamingConvention}', enqueue_options => enqueue_options, message_properties => message_properties, payload => TYPE_{dataBaseObjectsNamingConvention}(messageEnd, message_buffer), msgid => message_handle);" + Environment.NewLine;
        }

        private string PrepareEnqueueScript(ColumnInfo column, string dataBaseObjectsNamingConvention)
        {
            var messageType = $"'{dataBaseObjectsNamingConvention}/' || dmlType || '/{column.Name.Replace(Quotes, string.Empty)}'";

            if (column.Type.Contains("TIMESTAMP") && column.Type.EndsWith("WITH TIME ZONE"))
            {
                return PrepareEnqueueScriptForTimeStampWithTimeZone(column, messageType, dataBaseObjectsNamingConvention);
            }

            if (column.Type.Contains("TIMESTAMP") && column.Type.EndsWith("WITH LOCAL TIME ZONE"))
            {
                return PrepareEnqueueScriptForTimeStampWithLocalTimeZone(column, messageType, dataBaseObjectsNamingConvention);
            }

            if (column.Type == "DATE")
            {
                return PrepareEnqueueScriptForDate(column, messageType, dataBaseObjectsNamingConvention);
            }

            if (column.Type.Contains("TIMESTAMP"))
            {
                return PrepareEnqueueScriptForTimeStamp(column, messageType, dataBaseObjectsNamingConvention);
            }

            if (column.Type == "XMLTYPE")
            {
                return PrepareEnqueueScriptForXmlType(column, messageType, dataBaseObjectsNamingConvention);
            }

            if (column.Type == "RAW")
            {
                return PrepareEnqueueScriptForRawType(column, messageType, dataBaseObjectsNamingConvention);
            }

            if (column.Type == "NVARCHAR" || column.Type == "NVARCHAR2" || column.Type == "VARCHAR" || column.Type == "VARCHAR2")
            {
                return PrepareEnqueueScriptForVarchar(column, messageType, dataBaseObjectsNamingConvention);
            }

            if (column.Type == "NCHAR" || column.Type == "CHAR")
            {
                return PrepareEnqueueScriptForChar(column, messageType, dataBaseObjectsNamingConvention);
            }

            return PrepareEnqueueScriptForOther(column, messageType, dataBaseObjectsNamingConvention);
        }

        private static string PrepareEnqueueScriptForRawType(ColumnInfo column, string messageType, string dataBaseObjectsNamingConvention)
        {
            var variable = "v_" + column.Name.Replace(" ", "_").Replace(Quotes, string.Empty);
            return
                $"message_content:= TYPE_{dataBaseObjectsNamingConvention}({messageType}, EMPTY_BLOB());" + Environment.NewLine +
                $"DBMS_AQ.ENQUEUE(queue_name => 'QUE_{dataBaseObjectsNamingConvention}', enqueue_options => enqueue_options, message_properties => message_properties, payload => message_content, msgid => message_handle);" + Environment.NewLine +
                $"IF {variable} IS NOT NULL THEN" + Environment.NewLine +
                $"  SELECT t.user_data.message INTO lob_loc FROM QT_{dataBaseObjectsNamingConvention} t WHERE t.msgid = message_handle;" + Environment.NewLine +
                $"  DBMS_LOB.WRITE(lob_loc, UTL_RAW.LENGTH({variable}), 1, {variable});" + Environment.NewLine +
                $"END IF;" + Environment.NewLine;
        }

        private static string PrepareEnqueueScriptForXmlType(ColumnInfo column, string messageType, string dataBaseObjectsNamingConvention)
        {
            var variable = "v_" + column.Name.Replace(" ", "_").Replace(Quotes, string.Empty);
            return
                $"l_dest_offset := 1;" + Environment.NewLine +
                $"l_src_offset := 1;" + Environment.NewLine +
                $"l_csid := dbms_lob.default_csid;" + Environment.NewLine +
                $"l_ctx := dbms_lob.default_lang_ctx;" + Environment.NewLine +
                $"l_warn := 0;" + Environment.NewLine +
                $"message_content:= TYPE_{dataBaseObjectsNamingConvention}({messageType}, EMPTY_BLOB());" + Environment.NewLine +
                $"DBMS_AQ.ENQUEUE(queue_name => 'QUE_{dataBaseObjectsNamingConvention}', enqueue_options => enqueue_options, message_properties => message_properties, payload => message_content, msgid => message_handle);" + Environment.NewLine +
                $"IF {variable} IS NOT NULL THEN" + Environment.NewLine +
                $"  SELECT t.user_data.message INTO lob_loc FROM QT_{dataBaseObjectsNamingConvention} t WHERE t.msgid = message_handle;" + Environment.NewLine +
                $"  DBMS_LOB.CONVERTTOBLOB(lob_loc," + Environment.NewLine +
                $"      {variable}.GETCLOBVAL()," + Environment.NewLine +
                $"      DBMS_LOB.LOBMAXSIZE," + Environment.NewLine +
                $"      l_dest_offset," + Environment.NewLine +
                $"      l_src_offset," + Environment.NewLine +
                $"      l_csid," + Environment.NewLine +
                $"      l_ctx," + Environment.NewLine +
                $"      l_warn); " + Environment.NewLine +
                $"END IF;" + Environment.NewLine;
        }

        private static string PrepareEnqueueScriptForOther(ColumnInfo column, string messageType, string dataBaseObjectsNamingConvention)
        {
            var variable = "TO_CHAR(v_" + column.Name.Replace(" ", "_").Replace(Quotes, string.Empty) + ")";
            return
                $"message_content:= TYPE_{dataBaseObjectsNamingConvention}({messageType}, EMPTY_BLOB());" + Environment.NewLine +
                $"DBMS_AQ.ENQUEUE(queue_name => 'QUE_{dataBaseObjectsNamingConvention}', enqueue_options => enqueue_options, message_properties => message_properties, payload => message_content, msgid => message_handle);" + Environment.NewLine +
                $"IF {variable} IS NOT NULL THEN" + Environment.NewLine +
                $"  SELECT UTL_RAW.CAST_TO_RAW({variable}) INTO message_buffer FROM DUAL;" + Environment.NewLine +
                $"  SELECT t.user_data.message INTO lob_loc FROM QT_{dataBaseObjectsNamingConvention} t WHERE t.msgid = message_handle;" + Environment.NewLine +
                $"  DBMS_LOB.WRITE(lob_loc, UTL_RAW.LENGTH(message_buffer), 1, message_buffer);" + Environment.NewLine +
                $"END IF;" + Environment.NewLine;
        }

        private static string PrepareEnqueueScriptForVarchar(ColumnInfo column, string messageType, string dataBaseObjectsNamingConvention)
        {
            var variable = "v_" + column.Name.Replace(" ", "_").Replace(Quotes, string.Empty);
            return
                $"l_dest_offset := 1;" + Environment.NewLine +
                $"l_src_offset := 1;" + Environment.NewLine +
                $"l_csid := dbms_lob.default_csid;" + Environment.NewLine +
                $"l_ctx := dbms_lob.default_lang_ctx;" + Environment.NewLine +
                $"l_warn := 0;" + Environment.NewLine +
                $"l_clob := TO_CHAR({variable});" + Environment.NewLine +
                $"message_content:= TYPE_{dataBaseObjectsNamingConvention}({messageType}, EMPTY_BLOB());" + Environment.NewLine +
                $"DBMS_AQ.ENQUEUE(queue_name => 'QUE_{dataBaseObjectsNamingConvention}', enqueue_options => enqueue_options, message_properties => message_properties, payload => message_content, msgid => message_handle);" + Environment.NewLine +
                $"IF {variable} IS NOT NULL THEN" + Environment.NewLine +
                $"  SELECT t.user_data.message INTO lob_loc FROM QT_{dataBaseObjectsNamingConvention} t WHERE t.msgid = message_handle;" + Environment.NewLine +
                $"  DBMS_LOB.CONVERTTOBLOB(lob_loc," + Environment.NewLine +
                $"      l_clob," + Environment.NewLine +
                $"      DBMS_LOB.LOBMAXSIZE," + Environment.NewLine +
                $"      l_dest_offset," + Environment.NewLine +
                $"      l_src_offset," + Environment.NewLine +
                $"      l_csid," + Environment.NewLine +
                $"      l_ctx," + Environment.NewLine +
                $"      l_warn); " + Environment.NewLine +
                $"END IF;" + Environment.NewLine;
        }

        private static string PrepareEnqueueScriptForChar(ColumnInfo column, string messageType, string dataBaseObjectsNamingConvention)
        {
            var variable = "TO_CHAR(v_" + column.Name.Replace(" ", "_").Replace(Quotes, string.Empty) + ")";
            return
                $"message_content:= TYPE_{dataBaseObjectsNamingConvention}({messageType}, EMPTY_BLOB());" + Environment.NewLine +
                $"DBMS_AQ.ENQUEUE(queue_name => 'QUE_{dataBaseObjectsNamingConvention}', enqueue_options => enqueue_options, message_properties => message_properties, payload => message_content, msgid => message_handle);" + Environment.NewLine +
                $"IF {variable} IS NOT NULL THEN" + Environment.NewLine +
                $"  SELECT UTL_RAW.CAST_TO_RAW({variable}) INTO message_buffer FROM DUAL;" + Environment.NewLine +
                $"  SELECT t.user_data.message INTO lob_loc FROM QT_{dataBaseObjectsNamingConvention} t WHERE t.msgid = message_handle;" + Environment.NewLine +
                $"  DBMS_LOB.WRITE(lob_loc, UTL_RAW.LENGTH(message_buffer), 1, message_buffer);" + Environment.NewLine +
                $"END IF;" + Environment.NewLine;
        }

        private static string PrepareEnqueueScriptForTimeStampWithTimeZone(ColumnInfo column, string messageType, string dataBaseObjectsNamingConvention)
        {
            var variable = "TO_CHAR(v_" + column.Name.Replace(" ", "_").Replace(Quotes, string.Empty) + $", '{new DateTimeStampWithTimeZoneFormat().OracleFormat}')";
            return
                $"message_content:= TYPE_{dataBaseObjectsNamingConvention}({messageType}, EMPTY_BLOB());" + Environment.NewLine +
                $"DBMS_AQ.ENQUEUE(queue_name => 'QUE_{dataBaseObjectsNamingConvention}', enqueue_options => enqueue_options, message_properties => message_properties, payload => message_content, msgid => message_handle);" + Environment.NewLine +
                $"IF {variable} IS NOT NULL THEN" + Environment.NewLine +
                $"  SELECT UTL_RAW.CAST_TO_RAW({variable}) INTO message_buffer FROM DUAL;" + Environment.NewLine +
                $"  SELECT t.user_data.message INTO lob_loc FROM QT_{dataBaseObjectsNamingConvention} t WHERE t.msgid = message_handle;" + Environment.NewLine +
                $"  DBMS_LOB.WRITE(lob_loc, UTL_RAW.LENGTH(message_buffer), 1, message_buffer);" + Environment.NewLine +
                $"END IF;" + Environment.NewLine;
        }

        private static string PrepareEnqueueScriptForTimeStampWithLocalTimeZone(ColumnInfo column, string messageType, string dataBaseObjectsNamingConvention)
        {
            var variable = "TO_CHAR(v_" + column.Name.Replace(" ", "_").Replace(Quotes, string.Empty) + $", '{new DateTimeStampWithLocalTimeZoneFormat().OracleFormat}')";
            return
                $"message_content:= TYPE_{dataBaseObjectsNamingConvention}({messageType}, EMPTY_BLOB());" + Environment.NewLine +
                $"DBMS_AQ.ENQUEUE(queue_name => 'QUE_{dataBaseObjectsNamingConvention}', enqueue_options => enqueue_options, message_properties => message_properties, payload => message_content, msgid => message_handle);" + Environment.NewLine +
                $"IF {variable} IS NOT NULL THEN" + Environment.NewLine +
                $"  SELECT UTL_RAW.CAST_TO_RAW({variable}) INTO message_buffer FROM DUAL;" + Environment.NewLine +
                $"  SELECT t.user_data.message INTO lob_loc FROM QT_{dataBaseObjectsNamingConvention} t WHERE t.msgid = message_handle;" + Environment.NewLine +
                $"  DBMS_LOB.WRITE(lob_loc, UTL_RAW.LENGTH(message_buffer), 1, message_buffer);" + Environment.NewLine +
                $"END IF;" + Environment.NewLine;
        }

        private static string PrepareEnqueueScriptForTimeStamp(ColumnInfo column, string messageType, string dataBaseObjectsNamingConvention)
        {
            var variable = "TO_CHAR(v_" + column.Name.Replace(" ", "_").Replace(Quotes, string.Empty) + $", '{new DateStampFormat().OracleFormat}')";
            return
                $"message_content:= TYPE_{dataBaseObjectsNamingConvention}({messageType}, EMPTY_BLOB());" + Environment.NewLine +
                $"DBMS_AQ.ENQUEUE(queue_name => 'QUE_{dataBaseObjectsNamingConvention}', enqueue_options => enqueue_options, message_properties => message_properties, payload => message_content, msgid => message_handle);" + Environment.NewLine +
                $"IF {variable} IS NOT NULL THEN" + Environment.NewLine +
                $"  SELECT UTL_RAW.CAST_TO_RAW({variable}) INTO message_buffer FROM DUAL;" + Environment.NewLine +
                $"  SELECT t.user_data.message INTO lob_loc FROM QT_{dataBaseObjectsNamingConvention} t WHERE t.msgid = message_handle;" + Environment.NewLine +
                $"  DBMS_LOB.WRITE(lob_loc, UTL_RAW.LENGTH(message_buffer), 1, message_buffer);" + Environment.NewLine +
                $"END IF;" + Environment.NewLine;
        }

        private static string PrepareEnqueueScriptForDate(ColumnInfo column, string messageType, string dataBaseObjectsNamingConvention)
        {
            var variable = "TO_CHAR(v_" + column.Name.Replace(" ", "_").Replace(Quotes, string.Empty) + $", '{new DateFormat().OracleFormat}')";

            return
                $"message_content:= TYPE_{dataBaseObjectsNamingConvention}({messageType}, EMPTY_BLOB());" + Environment.NewLine +
                $"DBMS_AQ.ENQUEUE(queue_name => 'QUE_{dataBaseObjectsNamingConvention}', enqueue_options => enqueue_options, message_properties => message_properties, payload => message_content, msgid => message_handle);" + Environment.NewLine +
                $"IF {variable} IS NOT NULL THEN" + Environment.NewLine +
                $"  SELECT UTL_RAW.CAST_TO_RAW({variable}) INTO message_buffer FROM DUAL;" + Environment.NewLine +
                $"  SELECT t.user_data.message INTO lob_loc FROM QT_{dataBaseObjectsNamingConvention} t WHERE t.msgid = message_handle;" + Environment.NewLine +
                $"  DBMS_LOB.WRITE(lob_loc, UTL_RAW.LENGTH(message_buffer), 1, message_buffer);" + Environment.NewLine +
                $"END IF;" + Environment.NewLine;
        }

        private async Task WaitForNotifications(
            CancellationToken cancellationToken,
            Delegate[] onChangeSubscribedList,
            Delegate[] onErrorSubscribedList,
            Delegate[] onStatusChangedSubscribedList,
            string connectionString,
            string databaseObjectsNaming,
            int timeOutWatchDog,
            ModelToTableMapper<T> modelMapper,
            ICollection<string> processableMessages,
            bool automaticDatabaseObjectsTeardown,
            IEnumerable<ColumnInfo> userInterestedColumns,
            Encoding encoding = null)
        {
            this.WriteTraceMessage(TraceLevel.Verbose, "Get in WaitForNotifications.");
            NotifyListenersAboutStatus(onStatusChangedSubscribedList, TableDependencyStatus.Started);

            var task = default(Task);
            var getQueueMessageCommand = default(OracleCommand);
            var newMessageReadyToBeNotified = false;
            var messagesBag = new MessagesBag(encoding ?? Encoding.UTF8, string.Format(StartMessageTemplate, databaseObjectsNaming), string.Format(EndMessageTemplate, databaseObjectsNaming));

            try
            {
                while (true)
                {
                    if (automaticDatabaseObjectsTeardown)
                    {
                        this.WriteTraceMessage(TraceLevel.Verbose, "WatchDog Starting.");
                        StartWatchDog(connectionString, databaseObjectsNaming, timeOutWatchDog);
                        this.WriteTraceMessage(TraceLevel.Verbose, "WatchDog Started.");
                    }

                    task = Task.Factory.StartNew(() =>
                    {
                        using (var transactionScope = new TransactionScope(TransactionScopeOption.RequiresNew, TimeSpan.MaxValue, TransactionScopeAsyncFlowOption.Enabled))
                        {
                            this.WriteTraceMessage(TraceLevel.Verbose, "Transaction started.");

                            using (var connection = new OracleConnection(connectionString))
                            {
                                connection.Open();
                                this.WriteTraceMessage(TraceLevel.Verbose, "Connection opened.");

                                using (getQueueMessageCommand = connection.CreateCommand())
                                {
                                    getQueueMessageCommand.CommandText = $"DEQ_{databaseObjectsNaming}";
                                    getQueueMessageCommand.CommandType = CommandType.StoredProcedure;
                                    getQueueMessageCommand.CommandTimeout = 0;
                                    getQueueMessageCommand.Parameters.Add(new OracleParameter { ParameterName = "p_recordset", OracleDbType = OracleDbType.RefCursor, Direction = ParameterDirection.Output });

                                    NotifyListenersAboutStatus(onStatusChangedSubscribedList, TableDependencyStatus.WaitingForNotification);
                                    this.WriteTraceMessage(TraceLevel.Verbose, "Running stored to get message.");

                                    using (var reader = getQueueMessageCommand.ExecuteReader(CommandBehavior.CloseConnection))
                                    {
                                        while (reader.Read())
                                        {
                                            var messageType = reader.IsDBNull(0) ? null : reader.GetString(0);
                                            this.WriteTraceMessage(TraceLevel.Verbose, $"DB message received. Message type = {messageType}.");

                                            if (processableMessages.Contains(messageType))
                                            {
                                                var messageContent = reader.IsDBNull(1) ? null : (byte[]) reader[1];

                                                var messageStatus = messagesBag.AddMessage(messageType, messageContent);
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
                                                this.WriteTraceMessage(TraceLevel.Verbose, $"Message discarted [{messageType}].");
                                            }
                                        }
                                    }
                                }
                            }

                            if (newMessageReadyToBeNotified)
                            {
                                newMessageReadyToBeNotified = false;
                                RaiseEvent(onChangeSubscribedList, modelMapper, messagesBag, userInterestedColumns);
                                this.WriteTraceMessage(TraceLevel.Verbose, "Message notified.");

                                transactionScope.Complete();
                                NotifyListenersAboutStatus(onStatusChangedSubscribedList, TableDependencyStatus.MessageSent);
                                this.WriteTraceMessage(TraceLevel.Verbose, "Transaction completed.");
                            }
                        }
                    }, cancellationToken);

                    task.Wait(cancellationToken);
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
                if (automaticDatabaseObjectsTeardown)
                {
                    StopWatchDog(connectionString, databaseObjectsNaming);
                    this.WriteTraceMessage(TraceLevel.Verbose, "WatchDog Stopped.");
                }
            }

            if (getQueueMessageCommand != null)
            {
                var connection = getQueueMessageCommand.Connection;

                getQueueMessageCommand.Cancel();
                if (connection.State == ConnectionState.Open) connection.Close();
                getQueueMessageCommand.Dispose();
                connection.Dispose();

                this.WriteTraceMessage(TraceLevel.Verbose, "Closed command and connection.");
            }

            if (task?.Status == TaskStatus.Running)
            {
                task.Dispose();
                this.WriteTraceMessage(TraceLevel.Verbose, "Task disposed.");
            }

            this.WriteTraceMessage(TraceLevel.Verbose, "Exiting from WaitForNotifications.");
        }

        private static List<string> GetDmlTriggerType(DmlTriggerType dmlTriggerType)
        {
            var afters = new List<string>();
            if (dmlTriggerType.HasFlag(DmlTriggerType.All))
            {
                afters.Add(DmlTriggerType.Insert.ToString().ToUpper());
                afters.Add(DmlTriggerType.Update.ToString().ToUpper());
                afters.Add(DmlTriggerType.Delete.ToString().ToUpper());
            }
            else
            {
                if (dmlTriggerType.HasFlag(DmlTriggerType.Insert)) afters.Add(DmlTriggerType.Insert.ToString().ToUpper());
                if (dmlTriggerType.HasFlag(DmlTriggerType.Delete)) afters.Add(DmlTriggerType.Delete.ToString().ToUpper());
                if (dmlTriggerType.HasFlag(DmlTriggerType.Update)) afters.Add(DmlTriggerType.Update.ToString().ToUpper());
            }

            return afters;
        }

        private static void StartWatchDog(string connectionString, string databaseObjectsNaming, int timeOutWatchDog)
        {
            using (var connection = new OracleConnection(connectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"BEGIN DBMS_SCHEDULER.SET_ATTRIBUTE('JOB_{databaseObjectsNaming}', 'START_DATE', SYSTIMESTAMP + INTERVAL '{(timeOutWatchDog / 60)}' MINUTE); DBMS_SCHEDULER.ENABLE('JOB_{databaseObjectsNaming}'); END;";
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
                    watchDogDisableCommand.CommandText = $"BEGIN DBMS_SCHEDULER.DISABLE('JOB_{databaseObjectsNaming}', TRUE); DBMS_SCHEDULER.SET_ATTRIBUTE_NULL('JOB_{databaseObjectsNaming}', 'START_DATE'); END;";
                    watchDogDisableCommand.CommandType = CommandType.Text;
                    watchDogDisableCommand.ExecuteNonQuery();
                }
            }
        }

        private static void RaiseEvent(IEnumerable<Delegate> delegates, ModelToTableMapper<T> modelMapper, MessagesBag messagesBag, IEnumerable<ColumnInfo> userInterestedColumns)
        {
            if (delegates == null) return;
            foreach (var dlg in delegates.Where(d => d != null)) dlg.Method.Invoke(dlg.Target, new object[] { null, new OracleRecordChangedEventArgs<T>(messagesBag, modelMapper, userInterestedColumns) });
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

        private static void CheckIfUserHasPermission(OracleConnection connection)
        {
            var privilegesTable = new DataTable();
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "SELECT * FROM SESSION_PRIVS WHERE PRIVILEGE LIKE ('CREATE%') OR PRIVILEGE LIKE('DROP%')";
                privilegesTable.Load(command.ExecuteReader());
            }

            if (privilegesTable.Rows.Count == 0) throw new UserWithNoPermissionException();
            foreach (var permission in Enum.GetValues(typeof(OracleRequiredPermission)))
            {
                var permissionToCkeck = EnumUtil.GetDescriptionFromEnumValue((OracleRequiredPermission)permission);
                if (privilegesTable.AsEnumerable().All(r => r.Field<string>("PRIVILEGE").Replace("ANY ", string.Empty) != permissionToCkeck)) throw new UserWithNoPermissionException(permissionToCkeck);
            }

            var grantTable = new DataTable();
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "SELECT DISTINCT ALL_TAB_PRIVS.TABLE_NAME FROM ALL_TAB_PRIVS WHERE (TABLE_NAME = 'DBMS_AQADM' OR TABLE_NAME = 'DBMS_AQ' OR TABLE_NAME = 'DBMS_SCHEDULER') AND PRIVILEGE = 'EXECUTE' AND GRANTEE IN (SELECT USER FROM DUAL)";
                grantTable.Load(command.ExecuteReader());
            }

            if (grantTable.Rows.Count < 2) throw new UserWithNoGrantException("DBMS_AQADM or DBMS_AQ package or DBMS_SCHEDULER");
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

        private static string GetUpdateOfStatement(IEnumerable<ColumnInfo> tableColumns, IEnumerable<string> columnsUpdateOf)
        {
            if (columnsUpdateOf == null) return null;
            var updateOfList = columnsUpdateOf.Select(updateOf => tableColumns.Where(c => c.Name.ToUpper() == $"\"{updateOf.ToUpper()}\"").Select(c => c.Name).FirstOrDefault()).ToList();
            return " OF " + string.Join(", ", updateOfList);
        }

        private IEnumerable<ColumnInfo> CheckIfUserInterestedColumnsCanBeManaged(IEnumerable<ColumnInfo> tableColumnsToUse)
        {
            foreach (var tableColumn in tableColumnsToUse)
            {
                if (tableColumn.Type.ToUpper() == "BFILE" ||
                    tableColumn.Type.ToUpper() == "BLOB" ||
                    tableColumn.Type.ToUpper() == "CLOB" ||
                    tableColumn.Type.ToUpper() == "NLOB" ||
                    // {"ORA-04093: references to columns of type LONG are not allowed in triggers"}
                    tableColumn.Type.ToUpper() == "LONG ROW" ||
                    tableColumn.Type.ToUpper() == "LONG") throw new ColumnTypeNotSupportedException($"{tableColumn.Type} type is not an admitted for OracleTableDependency.");
            }

            return tableColumnsToUse;
        }

        private IEnumerable<ColumnInfo> PrivateGetUserInterestedColumns(IEnumerable<ColumnInfo> tableColumnsList)
        {
            var tableColumnsListFiltered = new List<ColumnInfo>();

            foreach (var entityPropertyInfo in ModelUtil.GetModelPropertiesInfo<T>())
            {
                var propertyMappedTo = _mapper?.GetMapping(entityPropertyInfo);
                var propertyName = propertyMappedTo ?? entityPropertyInfo.Name;

                // If model property is mapped to table column keep it
                foreach (var tableColumn in tableColumnsList)
                {
                    if (string.Equals(tableColumn.Name.ToLower(), Quotes + propertyName.ToLower() + Quotes, StringComparison.OrdinalIgnoreCase))
                    {
                        if (tableColumnsListFiltered.Any(ci => string.Equals(ci.Name, tableColumn.Name, StringComparison.OrdinalIgnoreCase)))
                        {
                            throw new ModelToTableMapperException("Model with columns having same name.");
                        }

                        tableColumnsListFiltered.Add(tableColumn);
                        break;
                    }
                }
            }

            return tableColumnsListFiltered;
        }

        private static void CheckUpdateOfValidity(IEnumerable<ColumnInfo> tableColumnsList, IEnumerable<string> updateOf)
        {
            if (updateOf != null)
            {
                if (!updateOf.Any()) throw new UpdateOfException("updateOf parameter is empty.");
                if (updateOf.Any(string.IsNullOrWhiteSpace)) throw new UpdateOfException("updateOf parameter contains a null or empty value.");

                var tableColumns = tableColumnsList as ColumnInfo[] ?? tableColumnsList.ToArray();
                var dbColumnNames = tableColumns.Select(t => t.Name.ToUpper()).ToList();
                foreach (var columnToMonitorDuringUpdate in updateOf.Where(columnToMonitor => !dbColumnNames.Contains(Quotes + columnToMonitor.ToUpper() + Quotes)))
                {
                    throw new UpdateOfException($"updateOf define column {columnToMonitorDuringUpdate} that does not exists.");
                }
            }
        }

        private static IEnumerable<ColumnInfo> GetTableColumnsList(string connectionString, string tableName)
        {
            var columnsList = new List<ColumnInfo>();

            using (var connection = new OracleConnection(connectionString))
            {
                connection.Open();
                using (var cmmand = connection.CreateCommand())
                {
                    cmmand.CommandText = $"SELECT COLUMN_NAME, DATA_TYPE, CHAR_LENGTH, CHAR_USED, DATA_PRECISION, DATA_SCALE, DATA_LENGTH FROM SYS.USER_TAB_COLUMNS WHERE UPPER(TABLE_NAME) = '{tableName.ToUpper()}' ORDER BY COLUMN_ID";
                    var reader = cmmand.ExecuteReader();
                    while (reader.Read()) columnsList.Add(PrepareColumnInfo(reader));
                }
            }

            return columnsList;
        }

        private static ColumnInfo PrepareColumnInfo(OracleDataReader reader)
        {
            var name = reader.GetString(0);
            var type = reader.GetString(1);

            if (type.StartsWith("DATE")) return new ColumnInfo(Quotes + name + Quotes, type);
            if (type.StartsWith("XMLTYPE")) return new ColumnInfo(Quotes + name + Quotes, type);
            if ((type.StartsWith("INTERVAL") || type.StartsWith("TIMESTAMP"))) return new ColumnInfo(Quotes + name + Quotes, type);

            var charLength = reader.IsDBNull(2) ? null : reader.GetValue(2).ToString();
            if (charLength != "0")
            {
                var charUsed = reader.IsDBNull(3) ? null : reader.GetValue(3).ToString();
                var size = "(" + charLength + (charUsed == "B" ? string.Empty : " CHAR") + ")";
                return new ColumnInfo(Quotes + name + Quotes, type, size);
            }

            var dataPrecision = reader.IsDBNull(4) ? null : reader.GetValue(4).ToString();
            if (dataPrecision != null)
            {
                var dataScale = reader.IsDBNull(5) ? null : reader.GetValue(5).ToString();
                var size = "(" + dataPrecision + (!string.IsNullOrWhiteSpace(dataScale) ? ", " + dataScale : string.Empty) + ")";
                return new ColumnInfo(Quotes + name + Quotes, type, size);
            }

            var dataLength = reader.IsDBNull(6) ? null : reader.GetValue(6).ToString();
            if (dataLength != null)
            {
                var size = "(" + dataLength + ")";
                return new ColumnInfo(Quotes + name + Quotes, type, size);
            }

            return new ColumnInfo(Quotes + name + Quotes, type);
        }

        private void CheckMapperValidity(IEnumerable<ColumnInfo> tableColumnsList)
        {
            if (_mapper != null)
            {
                if (_mapper.Count() < 1) throw new ModelToTableMapperException();

                // With ORACLE when define an column with "" it become case sensitive.
                var dbColumnNames = tableColumnsList.Select(t => t.Name.ToUpper().Replace(Quotes, string.Empty)).ToList();
                var mappingNames = _mapper.GetMappings().Select(t => t.Value.ToUpper()).ToList();

                mappingNames.ForEach<string>(mapping =>
                {
                    var found = false;
                    dbColumnNames.ForEach<string>(column =>
                    {
                        if (string.Compare(mapping, column, StringComparison.OrdinalIgnoreCase) == 0)
                        {
                            found = true;
                        }
                    });

                    if (!found)
                    {
                        throw new ModelToTableMapperException("Invalid mapper for property " + mapping);
                    }
                });
            }
        }
        #endregion
    }
}