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

#region Usings
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Security.Permissions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using TableDependency.Classes;
using TableDependency.Delegates;
using TableDependency.Enums;
using TableDependency.Exceptions;
using TableDependency.Extensions;
using TableDependency.Mappers;
using TableDependency.Messages;
using TableDependency.SqlClient.Enumerations;
using TableDependency.SqlClient.Extensions;
using TableDependency.SqlClient.EventArgs;
using TableDependency.SqlClient.Exceptions;
using TableDependency.SqlClient.Messages;
using TableDependency.SqlClient.Resources;
using TableDependency.Utilities;
using IsolationLevel = System.Transactions.IsolationLevel;

#endregion

namespace TableDependency.SqlClient
{
    /// <summary>
    /// SqlTableDependency class.
    /// </summary>
    public class SqlTableDependency<T> : TableDependency<T> where T : class
    {
        #region Private variables

        private const string Max = "MAX";
        private const string Comma = ",";
        private const string Space = " ";
        private const string MyName = "SqlTableDependency";
        private SqlServerVersion _sqlVersion = SqlServerVersion.Unknown;

        #endregion

        #region Properties

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
        public SqlTableDependency(string connectionString)
            : base(MyName, connectionString, null, null, (IList<string>)null, DmlTriggerType.All, true)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        public SqlTableDependency(string connectionString, string tableName)
            : base(MyName, connectionString, tableName, null, (IList<string>)null, DmlTriggerType.All, true, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="mapper">Model to columns table mapper.</param>
        public SqlTableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper)
            : base(MyName, connectionString, tableName, mapper, (IList<string>)null, DmlTriggerType.All, true, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="mapper">Model to columns table mapper.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        public SqlTableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper, IList<string> updateOf)
            : base(MyName, connectionString, tableName, mapper, updateOf, DmlTriggerType.All, true, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="mapper">Model to columns table mapper.</param>
        /// <param name="automaticDatabaseObjectsTeardown">Destroy all database objects created for receive notifications.</param>
        /// <param name="namingConventionForDatabaseObjects">The naming convention for database objects.</param>
        public SqlTableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
            : base(MyName, connectionString, tableName, mapper, (IList<string>)null, DmlTriggerType.All, automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="mapper">Model to columns table mapper.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        /// <param name="automaticDatabaseObjectsTeardown">Destroy all database objects created for receive notifications.</param>
        /// <param name="namingConventionForDatabaseObjects">The naming convention for database objects.</param>
        public SqlTableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper, IList<string> updateOf, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
            : base(MyName, connectionString, tableName, mapper, updateOf, DmlTriggerType.All, automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        /// <param name="automaticDatabaseObjectsTeardown">Destroy all database objects created for receive notifications.</param>
        /// <param name="namingConventionForDatabaseObjects">The naming convention for database objects.</param>
        public SqlTableDependency(string connectionString, string tableName, IList<string> updateOf, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
            : base(MyName, connectionString, tableName, null, updateOf, DmlTriggerType.All, automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="mapper">The mapper.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        /// <param name="dmlTriggerType">Type of the DML trigger.</param>
        /// <param name="automaticDatabaseObjectsTeardown">Destroy all database objects created for receive notifications.</param>
        /// <param name="namingConventionForDatabaseObjects">The naming convention for database objects.</param>
        public SqlTableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper, IList<string> updateOf, DmlTriggerType dmlTriggerType, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
            : base(MyName, connectionString, tableName, mapper, updateOf, dmlTriggerType, automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        /// <param name="automaticDatabaseObjectsTeardown">Destroy all database objects created for receive notifications.</param>
        /// <param name="namingConventionForDatabaseObjects">The naming convention for database objects.</param>
        public SqlTableDependency(string connectionString, IList<string> updateOf, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
            : base(MyName, connectionString, null, null, updateOf, DmlTriggerType.All, automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        public SqlTableDependency(string connectionString, IList<string> updateOf)
            : base(MyName, connectionString, null, null, updateOf, DmlTriggerType.All, true, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        public SqlTableDependency(string connectionString, string tableName, IList<string> updateOf)
            : base(MyName, connectionString, tableName, null, updateOf, DmlTriggerType.All, true, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="mapper">Model to columns table mapper.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        public SqlTableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper, UpdateOfModel<T> updateOf)
            : base(MyName, connectionString, tableName, mapper, updateOf, DmlTriggerType.All, true, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="mapper">Model to columns table mapper.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        /// <param name="automaticDatabaseObjectsTeardown">Destroy all database objects created for receive notifications.</param>
        /// <param name="namingConventionForDatabaseObjects">The naming convention for database objects.</param>
        public SqlTableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper, UpdateOfModel<T> updateOf, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
            : base(MyName, connectionString, tableName, mapper, updateOf, DmlTriggerType.All, automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="mapper">Model to columns table mapper.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        /// <param name="dmlTriggerType">Type of the DML trigger.</param>
        /// <param name="automaticDatabaseObjectsTeardown">Destroy all database objects created for receive notifications.</param>
        /// <param name="namingConventionForDatabaseObjects">The naming convention for database objects.</param>
        public SqlTableDependency(string connectionString, string tableName, ModelToTableMapper<T> mapper, UpdateOfModel<T> updateOf, DmlTriggerType dmlTriggerType, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
            : base(MyName, connectionString, tableName, mapper, updateOf, dmlTriggerType, automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table to monitor.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        /// <param name="automaticDatabaseObjectsTeardown">Destroy all database objects created for receive notifications.</param>
        /// <param name="namingConventionForDatabaseObjects">The naming convention for database objects.</param>
        public SqlTableDependency(string connectionString, string tableName, UpdateOfModel<T> updateOf, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
            : base(MyName, connectionString, tableName, null, updateOf, DmlTriggerType.All, automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        /// <param name="automaticDatabaseObjectsTeardown">Destroy all database objects created for receive notifications.</param>
        /// <param name="namingConventionForDatabaseObjects">The naming convention for database objects.</param>
        public SqlTableDependency(string connectionString, UpdateOfModel<T> updateOf, bool automaticDatabaseObjectsTeardown, string namingConventionForDatabaseObjects = null)
            : base(MyName, connectionString, null, null, updateOf, DmlTriggerType.All, automaticDatabaseObjectsTeardown, namingConventionForDatabaseObjects)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        public SqlTableDependency(string connectionString, string tableName, UpdateOfModel<T> updateOf)
            : base(MyName, connectionString, tableName, null, updateOf, DmlTriggerType.All, true, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlTableDependency{T}" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="updateOf">Column's names white list used to specify interested columns. Only when one of these columns is updated a notification is received.</param>
        public SqlTableDependency(string connectionString, UpdateOfModel<T> updateOf)
            : base(MyName, connectionString, null, null, updateOf, DmlTriggerType.All, true, null)
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

            base.Start(timeOut, watchDogTimeOut);

            var onChangedSubscribedList = OnChanged?.GetInvocationList();
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
                    _schemaName,
                    _dataBaseObjectsNamingConvention,
                    timeOut,
                    watchDogTimeOut,                    
                    _processableMessages,
                    _mapper,
                    _automaticDatabaseObjectsTeardown,
                    _userInterestedColumns,
                    this.Encoding),
                _cancellationTokenSource.Token);

            this.WriteTraceMessage(TraceLevel.Info, "Started waiting for notification.");
        }

        #endregion

        #region Protected methods

        protected override string GetCandidateTableName(string tableName)
        {
            if (!string.IsNullOrWhiteSpace(tableName))
            {
                if (tableName.Contains("."))
                {
                    var splitted = tableName.Split('.');
                    return splitted[1].Replace("[", string.Empty).Replace("]", string.Empty);
                }
                else
                {
                    return tableName.Replace("[", string.Empty).Replace("]", string.Empty);
                }
            }
            else
            {
                return (!string.IsNullOrWhiteSpace(GetTableNameFromTableDataAnnotation()) ? GetTableNameFromTableDataAnnotation() : typeof(T).Name);
            }
        }

        protected override string GetCandidateSchemaName(string tableName)
        {
            if (!string.IsNullOrWhiteSpace(tableName))
            {
                if (tableName.Contains("."))
                {
                    var splitted = tableName.Split('.');
                    return splitted[0].Trim() != string.Empty ? splitted[0].Replace("[", string.Empty).Replace("]", string.Empty) : string.Empty;
                }
                else
                {
                    return "dbo";
                }
            }
            else
            {
                return (!string.IsNullOrWhiteSpace(GetSchemaNameFromTableDataAnnotation()) ? GetSchemaNameFromTableDataAnnotation() : "dbo");
            }
        }

        internal SqlServerVersion GetSqlServerVersion(string connectionString)
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
                throw new SqlServerVersionNotSupported();
            }
            finally
            {
                sqlConnection.Close();
            }

            return SqlServerVersion.SqlServerLatest;
        }

        protected override IList<string> RetrieveProcessableMessages(IEnumerable<ColumnInfo> userInterestedColumns, string databaseObjectsNaming)
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
            var columnsForSelect = string.Join(Comma, interestedColumns.Select(c => $"[{c.Name}]").ToList());
            var columnsForUpdateOf = _updateOf != null ? string.Join(" OR ", _updateOf.Where(c => !string.IsNullOrWhiteSpace(c)).Distinct(StringComparer.CurrentCultureIgnoreCase).Select(c => $"UPDATE([{c}])").ToList()) : null;

            return CreateDatabaseObjects(connectionString, dataBaseObjectsNamingConvention, interestedColumns, columnsForTableVariable, columnsForSelect, columnsForUpdateOf);
        }

        protected override IEnumerable<ColumnInfo> GetUserInterestedColumns(IEnumerable<string> updateOf)
        {
            var tableColumns = GetTableColumnsList(_connectionString);
            var tableColumnsList = tableColumns as ColumnInfo[] ?? tableColumns.ToArray();
            if (!tableColumnsList.Any()) throw new NoColumnsException(_tableName);

            CheckUpdateOfValidity(tableColumnsList, updateOf);
            CheckMapperValidity(tableColumnsList);

            var userIterestedColumns = PrivateGetUserInterestedColumns(tableColumnsList);

            return CheckIfUserInterestedColumnsCanBeManaged(userIterestedColumns);
        }

        protected override string GeneratedataBaseObjectsNamingConvention(string namingConventionForDatabaseObjects)
        {
            string name = $"{_schemaName}_{_tableName}";
            return string.IsNullOrWhiteSpace(namingConventionForDatabaseObjects) ? $"{name}_{Guid.NewGuid()}" : namingConventionForDatabaseObjects;
        }

        protected override bool CheckIfNeedsToCreateDatabaseObjects()
        {
            var allObjectAlreadyPresent = new Dictionary<string, bool>();

            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"select count(*) from sys.triggers where name = N'tr_{_dataBaseObjectsNamingConvention}'";
                    allObjectAlreadyPresent.Add($"TRIGGERS with name 'tr_{_dataBaseObjectsNamingConvention}'", (int)sqlCommand.ExecuteScalar() > 0);

                    sqlCommand.CommandText = $"select count(*) from sys.procedures where name = N'{_dataBaseObjectsNamingConvention}_QueueActivation'";
                    allObjectAlreadyPresent.Add($"PROCEDURE with name '{_dataBaseObjectsNamingConvention}_QueueActivation'", (int)sqlCommand.ExecuteScalar() > 0);

                    sqlCommand.CommandText = $"select count(*) from sys.services where name = N'{_dataBaseObjectsNamingConvention}'";
                    allObjectAlreadyPresent.Add($"SERVICE BROKER with name '{_dataBaseObjectsNamingConvention}'", (int)sqlCommand.ExecuteScalar() > 0);

                    sqlCommand.CommandText = $"select count(*) from sys.service_queues where name = N'{_dataBaseObjectsNamingConvention}'";
                    allObjectAlreadyPresent.Add($"QUEUE with name '{_dataBaseObjectsNamingConvention}'", (int)sqlCommand.ExecuteScalar() > 0);

                    sqlCommand.CommandText = $"select count(*) from sys.service_contracts where name = N'{_dataBaseObjectsNamingConvention}'";
                    allObjectAlreadyPresent.Add($"CONTRACT with name '{_dataBaseObjectsNamingConvention}'", (int)sqlCommand.ExecuteScalar() > 0);

                    sqlCommand.CommandText = "select count(*) from sys.service_message_types where name = N'" + string.Format(StartMessageTemplate, _dataBaseObjectsNamingConvention, ChangeType.Insert) + "'";
                    allObjectAlreadyPresent.Add("MESSAGE TYPE with name '" + string.Format(StartMessageTemplate, _dataBaseObjectsNamingConvention, ChangeType.Insert) + "'", (int)sqlCommand.ExecuteScalar() > 0);

                    sqlCommand.CommandText = "select count(*) from sys.service_message_types where name = N'" + string.Format(StartMessageTemplate, _dataBaseObjectsNamingConvention, ChangeType.Update) + "'";
                    allObjectAlreadyPresent.Add("MESSAGE TYPE with name '" + string.Format(StartMessageTemplate, _dataBaseObjectsNamingConvention, ChangeType.Update) + "'", (int)sqlCommand.ExecuteScalar() > 0);

                    sqlCommand.CommandText = "select count(*) from sys.service_message_types where name = N'" + string.Format(StartMessageTemplate, _dataBaseObjectsNamingConvention, ChangeType.Delete) + "'";
                    allObjectAlreadyPresent.Add("MESSAGE TYPE with name '" + string.Format(StartMessageTemplate, _dataBaseObjectsNamingConvention, ChangeType.Delete) + "'", (int)sqlCommand.ExecuteScalar() > 0);

                    foreach (var userInterestedColumn in _userInterestedColumns)
                    {
                        sqlCommand.CommandText = "select count(*) from sys.service_message_types where name = N'" + $"{_dataBaseObjectsNamingConvention}/{userInterestedColumn.Name}" + "'";
                        allObjectAlreadyPresent.Add("MESSAGE TYPE with name '" + $"{_dataBaseObjectsNamingConvention}/{userInterestedColumn.Name}" + "'", (int)sqlCommand.ExecuteScalar() > 0);
                    }
                }
            }

            if (allObjectAlreadyPresent.All(exist => !exist.Value)) return true;
            if (allObjectAlreadyPresent.All(exist => exist.Value)) return false;

            throw new SomeDatabaseObjectsNotPresentException(allObjectAlreadyPresent);
        }

        protected override void DropDatabaseObjects(string connectionString, string databaseObjectsNaming)
        {
            var dropMessageStartEnd = new List<string>()
            {
                $"IF EXISTS (SELECT * FROM sys.service_message_types WHERE name = N'{string.Format(StartMessageTemplate, databaseObjectsNaming, ChangeType.Insert)}') DROP MESSAGE TYPE [{string.Format(StartMessageTemplate, databaseObjectsNaming, ChangeType.Insert)}];",
                $"IF EXISTS (SELECT * FROM sys.service_message_types WHERE name = N'{string.Format(StartMessageTemplate, databaseObjectsNaming, ChangeType.Update)}') DROP MESSAGE TYPE [{string.Format(StartMessageTemplate, databaseObjectsNaming, ChangeType.Update)}];",
                $"IF EXISTS (SELECT * FROM sys.service_message_types WHERE name = N'{string.Format(StartMessageTemplate, databaseObjectsNaming, ChangeType.Delete)}') DROP MESSAGE TYPE [{string.Format(StartMessageTemplate, databaseObjectsNaming, ChangeType.Delete)}];"
            };

            var dropContracts = _userInterestedColumns
                .Select(c => $"IF EXISTS (SELECT * FROM sys.service_message_types WHERE name = N'{databaseObjectsNaming}/{c.Name}') DROP MESSAGE TYPE [{databaseObjectsNaming}/{c.Name}];" + Environment.NewLine)
                .Concat(dropMessageStartEnd)
                .ToList();

            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandText = string.Format(Scripts.ScriptDropAll, databaseObjectsNaming, string.Join(Environment.NewLine, dropContracts), _schemaName);
                    sqlCommand.ExecuteNonQuery();
                }
            }

            this.WriteTraceMessage(TraceLevel.Info, "Database objects destroyed.");
        }

        protected override void PreliminaryChecks(string connectionString, string tableName)
        {
            CheckIfConnectionStringIsValid(connectionString);
            CheckIfUserHasPermissions(connectionString);
            CheckIfServiceBrokerIsEnabled(connectionString);
            CheckIfTableExists(connectionString, tableName);

            _sqlVersion = this.GetSqlServerVersion(connectionString);
            if (_sqlVersion == SqlServerVersion.SqlServer2000) throw new SqlServerVersionNotSupported(SqlServerVersion.SqlServer2000);
        }

        #endregion

        #region Private methods

        private IList<string> CreateDatabaseObjects(string connectionString, string databaseObjectsNaming, IEnumerable<ColumnInfo> userInterestedColumns, string tableColumns, string selectColumns, string updateColumns)
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

                        var interestedColumns = userInterestedColumns as ColumnInfo[] ?? userInterestedColumns.ToArray();
                        foreach (var userInterestedColumn in interestedColumns)
                        {
                            var message = $"{databaseObjectsNaming}/{userInterestedColumn.Name}";
                            sqlCommand.CommandText = $"CREATE MESSAGE TYPE [{message}] VALIDATION = NONE;";
                            sqlCommand.ExecuteNonQuery();
                            processableMessages.Add(message);
                        }
                        this.WriteTraceMessage(TraceLevel.Verbose, "Messages type created.");

                        var contractBody = string.Join(Comma + Environment.NewLine, processableMessages.Select(message => $"[{message}] SENT BY INITIATOR"));
                        sqlCommand.CommandText = $"CREATE CONTRACT [{databaseObjectsNaming}] ({contractBody})";
                        sqlCommand.ExecuteNonQuery();
                        this.WriteTraceMessage(TraceLevel.Verbose, "Contract created.");

                        var dropMessages = string.Join(Environment.NewLine, processableMessages.Select(c => string.Format("IF EXISTS (SELECT * FROM sys.service_message_types WHERE name = N'{0}') DROP MESSAGE TYPE[{0}];", c)));
                        var dropAllScript = string.Format(Scripts.ScriptDropAll, databaseObjectsNaming, dropMessages, _schemaName);
                        sqlCommand.CommandText = string.Format(Scripts.CreateProcedureQueueActivation, databaseObjectsNaming, dropAllScript, _schemaName);
                        sqlCommand.ExecuteNonQuery();
                        this.WriteTraceMessage(TraceLevel.Verbose, "Procedure Queue Activation created.");

                        sqlCommand.CommandText = _sqlVersion == SqlServerVersion.SqlServer2005
                            ? $"CREATE QUEUE {_schemaName}.[{databaseObjectsNaming}] WITH STATUS = ON, RETENTION = OFF, ACTIVATION (PROCEDURE_NAME = {_schemaName}.[{databaseObjectsNaming}_QueueActivation], MAX_QUEUE_READERS = 1, EXECUTE AS OWNER)"
                            : $"CREATE QUEUE {_schemaName}.[{databaseObjectsNaming}] WITH STATUS = ON, RETENTION = OFF, POISON_MESSAGE_HANDLING (STATUS = OFF), ACTIVATION (PROCEDURE_NAME = {_schemaName}.[{databaseObjectsNaming}_QueueActivation], MAX_QUEUE_READERS = 1, EXECUTE AS OWNER)";
                        sqlCommand.ExecuteNonQuery();
                        this.WriteTraceMessage(TraceLevel.Verbose, "Queue created.");

                        sqlCommand.CommandText = $"CREATE SERVICE [{databaseObjectsNaming}] ON QUEUE {_schemaName}.[{databaseObjectsNaming}] ([{databaseObjectsNaming}])";
                        sqlCommand.ExecuteNonQuery();
                        this.WriteTraceMessage(TraceLevel.Verbose, "Service created.");

                        var declareVariableStatement = PrepareDeclareVariableStatement(interestedColumns);
                        var selectForSetVariablesStatement = PrepareSelectForSetVarialbes(interestedColumns);
                        var sendInsertConversationStatements = PrepareSendConversation(databaseObjectsNaming, ChangeType.Insert, interestedColumns);
                        var sendUpdatedConversationStatements = PrepareSendConversation(databaseObjectsNaming, ChangeType.Update, interestedColumns);
                        var sendDeletedConversationStatements = PrepareSendConversation(databaseObjectsNaming, ChangeType.Delete, interestedColumns);
                        var whereStatement = PrepareWhereStatement(interestedColumns);
                        var bodyForUpdate = !string.IsNullOrEmpty(updateColumns)
                            ? string.Format(Scripts.TriggerUpdateWithColumns, updateColumns, _tableName, selectColumns, ChangeType.Update, whereStatement)
                            : string.Format(Scripts.TriggerUpdateWithoutColumns, _tableName, selectColumns, ChangeType.Update, whereStatement);

                        sqlCommand.CommandText = string.Format(
                            Scripts.CreateTrigger,
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
                            string.Join(Comma, GetDmlTriggerType(_dmlTriggerType)));

                        sqlCommand.ExecuteNonQuery();
                        this.WriteTraceMessage(TraceLevel.Verbose, "Trigger created.");
                    }
                }

                transactionScope.Complete();
            }

            this.WriteTraceMessage(TraceLevel.Info, $"Database objects created with naming {databaseObjectsNaming}.");

            processableMessages.Add(SqlMessageTypes.EndDialogType);

            return processableMessages;
        }

        private static string PrepareWhereStatement(IEnumerable<ColumnInfo> userInterestedColumns)
        {
            var interestedColumns = userInterestedColumns as ColumnInfo[] ?? userInterestedColumns.ToArray();
            if (interestedColumns.Any(tableColumn =>
                string.Equals(tableColumn.Type.ToLowerInvariant(), "timestamp", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(tableColumn.Type.ToLowerInvariant(), "rowversion", StringComparison.OrdinalIgnoreCase))) return string.Empty;

            var separatorNewColumns = new Separator(2, Comma);
            var sBuilderNewColumns = new StringBuilder();
            var separatorOldColumns = new Separator(2, Comma);
            var sBuilderOldColumns = new StringBuilder();

            foreach (var column in interestedColumns)
            {
                sBuilderNewColumns.Append($"{separatorNewColumns.GetSeparator()}[m_New].[{column.Name}]");
                sBuilderOldColumns.Append($"{separatorOldColumns.GetSeparator()}[m_Old].[{column.Name}]");
            }

            return string.Format(
                Environment.NewLine + "WHERE NOT EXISTS(SELECT 1 FROM INSERTED AS [m_New] INNER JOIN DELETED AS [m_Old] ON BINARY_CHECKSUM({0}) = BINARY_CHECKSUM({1}))",
                sBuilderNewColumns,
                sBuilderOldColumns);
        }

        private static IEnumerable<string> GetDmlTriggerType(DmlTriggerType dmlTriggerType)
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

        private static CultureInfo GetDbCulture(string connectionString)
        {
            var cultureInfo = CultureInfo.CurrentCulture;

            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = "SELECT lcid FROM sys.syslanguages WHERE name = @@LANGUAGE";
                    var lcid = sqlCommand.ExecuteScalar() as int?;
                    if (lcid.HasValue) cultureInfo = new CultureInfo(lcid.Value, false);
                }
            }

            return cultureInfo;
        }

        private async Task WaitForNotifications(
            CancellationToken cancellationToken,
            Delegate[] onChangeSubscribedList,
            Delegate[] onErrorSubscribedList,
            Delegate[] onStatusChangedSubscribedList,
            string connectionString,
            string schemaName,
            string databaseObjectsNaming,
            int timeOut,
            int timeOutWatchDog,
            ICollection<string> processableMessages,
            ModelToTableMapper<T> modelMapper,
            bool automaticDatabaseObjectsTeardown,
            IEnumerable<ColumnInfo> userInterestedColumns,
            Encoding encoding)
        {
            var waitforSqlScript = $"WAITFOR(receive top ({processableMessages.Count}) [conversation_handle], [message_type_name], [message_body] FROM {schemaName}.[{databaseObjectsNaming}]), timeout {timeOut * 1000};";
            var newMessageReadyToBeNotified = false;
            this.WriteTraceMessage(TraceLevel.Verbose, "Get in WaitForNotifications.");            
           
            if (automaticDatabaseObjectsTeardown)
            {
                var dialogHandle = BeginDialogConversation(connectionString, databaseObjectsNaming);
                waitforSqlScript = $"begin conversation timer ('{dialogHandle}') timeout = {timeOutWatchDog};" + waitforSqlScript;
            }
            
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
                                this.WriteTraceMessage(TraceLevel.Verbose, "Running waitfor command.");

                                using (var sqlDataReader = await sqlCommand.ExecuteReaderAsync(cancellationToken).WithCancellation(cancellationToken))
                                {
                                    while (sqlDataReader.Read())
                                    {
                                        this.WriteTraceMessage(TraceLevel.Verbose, "Message received.");

                                        var messageType = sqlDataReader.IsDBNull(1) ? null : sqlDataReader.GetSqlString(1);
                                        if (messageType.Value == SqlMessageTypes.ErrorType)
                                        {
                                            this.WriteTraceMessage(TraceLevel.Verbose, $"Invalid message type [{messageType.Value}].");
                                            EndConversation(sqlConnection, sqlDataReader.GetSqlGuid(0));

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

            this.WriteTraceMessage(TraceLevel.Verbose, "Exiting from WaitForNotifications.");
        }

        private static MessagesBag CreateMessagesBag(string databaseObjectsNaming, Encoding encoding)
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

        private static Guid BeginDialogConversation(string connectionString, string databaseObjectsNaming)
        {
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    var sqlParameter = new SqlParameter { ParameterName = "@handle", DbType = DbType.Guid, Direction = ParameterDirection.Output };

                    sqlCommand.CommandText = string.Format("begin dialog conversation @handle from service [{0}] to service '{0}', 'CURRENT DATABASE' on contract [{0}] with encryption = off;", databaseObjectsNaming);
                    sqlCommand.Parameters.Add(sqlParameter);
                    sqlCommand.ExecuteNonQuery();
                    var dialogHandle = (Guid)sqlParameter.Value;

                    return dialogHandle;
                }
            }
        }

        private static void BeginConversationTimer(string connectionString, SqlGuid dialogHandle, int timeOutWatchDog)
        {
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"begin conversation timer ('{dialogHandle}') timeout = {timeOutWatchDog};";
                    sqlCommand.CommandTimeout = 0;
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        private static void EndConversation(SqlConnection sqlConnection, SqlGuid handle)
        {
            using (var sqlCommand = sqlConnection.CreateCommand())
            {
                sqlCommand.CommandText = "end conversation @handle";
                sqlCommand.Parameters.Add("@handle", SqlDbType.UniqueIdentifier);
                sqlCommand.Parameters["@handle"].Value = handle;
                sqlCommand.ExecuteNonQuery();
            }
        }

        private static string PrepareColumnListForTableVariable(IEnumerable<ColumnInfo> tableColumns)
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

            return string.Join(Comma, columns.ToList());
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

        private static void NotifyListenersAboutChange(IEnumerable<Delegate> delegates, ModelToTableMapper<T> modelMapper, MessagesBag messagesBag, IEnumerable<ColumnInfo> userInterestedColumns)
        {
            if (delegates == null) return;

            foreach (var dlg in delegates.Where(d => d != null))
            {
                try
                {
                    dlg.Method.Invoke(dlg.Target, new object[] { null, new SqlRecordChangedEventArgs<T>(messagesBag, modelMapper, userInterestedColumns) });
                }
                catch
                {
                    // ignored
                }
            }
        }

        private static string ComputeSize(string dataType, string characterMaximumLength, string numericPrecision, string numericScale, string dateTimePrecisione)
        {
            if (
                string.Equals(dataType.ToUpperInvariant(), "BINARY", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(dataType.ToUpperInvariant(), "VARBINARY", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(dataType.ToUpperInvariant(), "CHAR", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(dataType.ToUpperInvariant(), "NCHAR", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(dataType.ToUpperInvariant(), "VARCHAR", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(dataType.ToUpperInvariant(), "NVARCHAR", StringComparison.OrdinalIgnoreCase))
            {
                return characterMaximumLength == "-1" ? Max : characterMaximumLength;
            }

            if (string.Equals(dataType.ToUpperInvariant(), "DECIMAL", StringComparison.OrdinalIgnoreCase))
            {
                return $"{numericPrecision},{numericScale}";
            }

            if (
                string.Equals(dataType.ToUpperInvariant(), "DATETIME2", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(dataType.ToUpperInvariant(), "DATETIMEOFFSET", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(dataType.ToUpperInvariant(), "TIME", StringComparison.OrdinalIgnoreCase))
            {
                return $"{dateTimePrecisione}";
            }

            return null;
        }

        private IEnumerable<ColumnInfo> GetTableColumnsList(string connectionString)
        {
            var columnsList = new List<ColumnInfo>();

            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{_tableName}' AND TABLE_SCHEMA = '{_schemaName}' ORDER BY ORDINAL_POSITION";
                    var reader = sqlCommand.ExecuteReader();
                    while (reader.Read())
                    {
                        var name = reader.GetString(0);
                        var type = reader.GetString(1).ConvertNumericType();
                        var size = ComputeSize(type, reader.GetSafeString(2), reader.GetSafeString(3), reader.GetSafeString(4), reader.GetSafeString(5));
                        columnsList.Add(new ColumnInfo(name, type, size));
                    }
                }
            }

            return columnsList;
        }

        private void CheckMapperValidity(IEnumerable<ColumnInfo> tableColumnsList)
        {
            if (this._mapper == null) return;

            if (this._mapper.Count() < 1) throw new ModelToTableMapperException();

            var dbColumnNames = tableColumnsList.Select(t => t.Name.ToLowerInvariant()).ToList();

            if (this._mapper.GetMappings().Select(t => t.Value).Any(mappingColumnName => !dbColumnNames.Contains(mappingColumnName.ToLowerInvariant())))
            {
                throw new ModelToTableMapperException();
            }
        }

        private static IEnumerable<ColumnInfo> CheckIfUserInterestedColumnsCanBeManaged(IEnumerable<ColumnInfo> tableColumnsToUse)
        {
            var checkIfUserInterestedColumnsCanBeManaged = tableColumnsToUse as ColumnInfo[] ?? tableColumnsToUse.ToArray();
            foreach (var tableColumn in checkIfUserInterestedColumnsCanBeManaged)
            {
                if (
                    string.Equals(tableColumn.Type.ToUpperInvariant(), "IMAGE", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(tableColumn.Type.ToUpperInvariant(), "TEXT", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(tableColumn.Type.ToUpperInvariant(), "NTEXT", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(tableColumn.Type.ToUpperInvariant(), "STRUCTURED", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(tableColumn.Type.ToUpperInvariant(), "GEOGRAPHY", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(tableColumn.Type.ToUpperInvariant(), "GEOMETRY", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(tableColumn.Type.ToUpperInvariant(), "HIERARCHYID", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(tableColumn.Type.ToUpperInvariant(), "SQL_VARIANT", StringComparison.OrdinalIgnoreCase))
                {
                    throw new ColumnTypeNotSupportedException($"{tableColumn.Type} type is not an admitted for SqlTableDependency.");
                }
            }

            return checkIfUserInterestedColumnsCanBeManaged;
        }

        private static string ConvertFormat(ColumnInfo userInterestedColumn)
        {
            return (string.Equals(userInterestedColumn.Type, "datetime", StringComparison.OrdinalIgnoreCase) || string.Equals(userInterestedColumn.Type, "date", StringComparison.OrdinalIgnoreCase)) ? ", 121" : string.Empty;
        }

        private static string ConvertValueByType(ColumnInfo userInterestedColumn)
        {
            if (string.Equals(userInterestedColumn.Type, "binary", StringComparison.OrdinalIgnoreCase) || string.Equals(userInterestedColumn.Type, "varbinary", StringComparison.OrdinalIgnoreCase) || string.Equals(userInterestedColumn.Type, "timestamp", StringComparison.OrdinalIgnoreCase))
            {
                return $"@{userInterestedColumn.Name.Replace(Space, string.Empty)}";
            }

            return $"convert(nvarchar(max), @{userInterestedColumn.Name.Replace(Space, string.Empty)}{ConvertFormat(userInterestedColumn)})";
        }

        private static string PrepareSendConversation(string databaseObjectsNaming, ChangeType dmlType, IEnumerable<ColumnInfo> userInterestedColumns)
        {
            var sendList = userInterestedColumns
                .Select(insterestedColumn => $"IF @{insterestedColumn.Name.Replace(Space, string.Empty)} IS NOT NULL BEGIN" + Environment.NewLine + $";send on conversation @h message type[{databaseObjectsNaming}/{insterestedColumn.Name}] ({ConvertValueByType(insterestedColumn)})" + Environment.NewLine + "END" + Environment.NewLine + "ELSE BEGIN" + Environment.NewLine + $";send on conversation @h message type[{databaseObjectsNaming}/{insterestedColumn.Name}] (0x)" + Environment.NewLine + "END")
                .ToList();

            sendList.Insert(0, $";send on conversation @h message type[{string.Format(StartMessageTemplate, databaseObjectsNaming, dmlType)}] (convert(nvarchar, @dmlType))" + Environment.NewLine);

            return string.Join(Environment.NewLine, sendList);
        }

        private static string PrepareSelectForSetVarialbes(IEnumerable<ColumnInfo> userInterestedColumns)
        {
            return string.Join(Comma, userInterestedColumns.Select(insterestedColumn => $"@{insterestedColumn.Name.Replace(Space, string.Empty)} = [{insterestedColumn.Name}]"));
        }

        private static string PrepareDeclareVariableStatement(IEnumerable<ColumnInfo> userInterestedColumns)
        {
            var colonne = (from insterestedColumn in userInterestedColumns let variableName = insterestedColumn.Name.Replace(Space, string.Empty) let variableType = $"{insterestedColumn.Type.ToLowerInvariant()}" + (string.IsNullOrWhiteSpace(insterestedColumn.Size) ? string.Empty : $"({insterestedColumn.Size})") select $"DECLARE @{variableName} {variableType.ToLowerInvariant()}").ToList();
            return string.Join(Environment.NewLine, colonne);
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
            }
        }

        private static void CheckIfUserHasPermissions(string connectionString)
        {
            try
            {
                new SqlClientPermission(PermissionState.Unrestricted).Demand();
            }
            catch (Exception exception)
            {
                throw new UserWithNoPermissionException(exception);
            }

            var privilegesTable = new DataTable();
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = "SELECT [permission_name] FROM fn_my_permissions(NULL, 'DATABASE')";
                    privilegesTable.Load(sqlCommand.ExecuteReader(CommandBehavior.CloseConnection));
                }
            }

            if (privilegesTable.Rows.Count == 0) throw new UserWithNoPermissionException();
            foreach (var permission in Enum.GetValues(typeof(SqlServerRequiredPermission)))
            {
                var permissionToCkeck = EnumUtil.GetDescriptionFromEnumValue((SqlServerRequiredPermission)permission);
                if (privilegesTable.AsEnumerable().All(r => !string.Equals(r.Field<string>("permission_name"), permissionToCkeck, StringComparison.OrdinalIgnoreCase))) throw new UserWithNoPermissionException(permissionToCkeck);
            }

            var selectGratnOnSystemView = new DataTable();
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandType = CommandType.StoredProcedure;
                    sqlCommand.CommandText = "sp_helprotect";
                    selectGratnOnSystemView.Load(sqlCommand.ExecuteReader(CommandBehavior.CloseConnection));
                }
            }

            if (selectGratnOnSystemView.Rows.Count == 0) throw new UserWithNoPermissionException();
            foreach (var permissionToCkeck in Enum.GetValues(typeof(SqlServerSelectGrantOnSysView))
                .Cast<object>()
                .Select(view => EnumUtil.GetDescriptionFromEnumValue((SqlServerSelectGrantOnSysView)view))
                .Where(permissionToCkeck => selectGratnOnSystemView.AsEnumerable().All(r => !string.Equals(r.Field<string>("Object"), permissionToCkeck, StringComparison.OrdinalIgnoreCase))))
            {
                throw new UserWithNoPermissionException("SELECT on SYS." + permissionToCkeck + " view ");
            }
        }

        private static void CheckIfServiceBrokerIsEnabled(string connectionString)
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

        private void CheckIfTableExists(string connection, string tableName)
        {
            using (var sqlConnection = new SqlConnection(connection))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{_tableName}' AND TABLE_SCHEMA = '{_schemaName}'";
                    if ((int)sqlCommand.ExecuteScalar() == 0) throw new NotExistingTableException(tableName);
                }
            }
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
                    if (string.Equals(tableColumn.Name.ToLowerInvariant(), propertyName.ToLowerInvariant(), StringComparison.OrdinalIgnoreCase))
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
            if (updateOf == null) return;

            var columnsToMonitorDuringUpdate = updateOf as string[] ?? updateOf.ToArray();
            if (!columnsToMonitorDuringUpdate.Any()) throw new UpdateOfException("updateOf parameter is empty.");

            if (columnsToMonitorDuringUpdate.Any(string.IsNullOrWhiteSpace))
            {
                throw new UpdateOfException("updateOf parameter contains a null or empty value.");
            }

            var dbColumnNames = tableColumnsList.Select(t => t.Name.ToLowerInvariant()).ToList();
            foreach (var columnToMonitorDuringUpdate in columnsToMonitorDuringUpdate.Where(columnToMonitor => !dbColumnNames.Contains(columnToMonitor.ToLowerInvariant())))
            {
                throw new UpdateOfException($"Column '{columnToMonitorDuringUpdate}' specified in updateOf list does not exists.");
            }
        }

        #endregion
    }
}