#region License

// TableDependency, SqlTableDependency
// Copyright (c) 2015-2020 Christian Del Bianco. All rights reserved.
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

namespace TableDependency.SqlClient.Resources;

public static partial class SqlScripts
{
    public const string CreateProcedureQueueActivation = @"CREATE OR ALTER PROCEDURE [{2}].[{0}_QueueActivationSender]
WITH EXECUTE AS SELF
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @h AS UNIQUEIDENTIFIER;
    DECLARE @mt NVARCHAR(200);

    RECEIVE TOP(1) @h = conversation_handle, @mt = message_type_name FROM [{2}].[{0}_Sender];

    IF @mt = N'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog'
    BEGIN
        END CONVERSATION @h;
    END

    IF @mt = N'http://schemas.microsoft.com/SQL/ServiceBroker/DialogTimer' OR @mt = N'http://schemas.microsoft.com/SQL/ServiceBroker/Error'
    BEGIN
        PRINT N'SqlTableDependency: Drop objects {0} started.';

        END CONVERSATION @h;

        {1}

        PRINT N'SqlTableDependency: Drop objects {0} ended.';
    END
END";

    // Triggers run, by default, under the security context of the principal who caused the trigger to fire.
    // In order to change this behavior, you'll need to create the trigger using the WITH EXECUTE AS OWNER clause.
    // Below is an example which shows how that works. WITH EXECUTE AS OWNER allows the trigger to run in the security context of the database owner,
    // instead of the principal who is updating the table.
    //
    // EXECUTE AS SELF is equivalent to EXECUTE AS user_name, where the specified user is the person creating or altering the module.
    public const string CreateTrigger = @"CREATE OR ALTER TRIGGER [tr_{0}_Sender] ON {1}
WITH EXECUTE AS SELF
{20} {13} AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @rowsToProcess INT
    DECLARE @currentRow INT
    DECLARE @records XML
    DECLARE @theMessageContainer NVARCHAR(MAX)
    DECLARE @dmlType NVARCHAR(10)
    DECLARE @modifiedRecordsTable TABLE ([RowNumber] INT IDENTITY(1, 1), {2})
    DECLARE @exceptTable TABLE ([RowNumber] INT, {17})
	DECLARE @deletedTable TABLE ([RowNumber] INT IDENTITY(1, 1), {18})
    DECLARE @insertedTable TABLE ([RowNumber] INT IDENTITY(1, 1), {18})
    {5}

    {19}

    IF NOT EXISTS(SELECT 1 FROM INSERTED)
    BEGIN
        {21}
        SET @dmlType = '{12}'
        INSERT INTO @modifiedRecordsTable SELECT {3} FROM DELETED {14}
    END
    ELSE
    BEGIN
        IF NOT EXISTS(SELECT * FROM DELETED)
        BEGIN
            {22}
            SET @dmlType = '{10}'
            INSERT INTO @modifiedRecordsTable SELECT {3} FROM INSERTED {14}
        END
        ELSE
        BEGIN
            {4}
        END
    END

    SELECT @rowsToProcess = COUNT(1) FROM @modifiedRecordsTable

    BEGIN TRY
        WHILE @rowsToProcess > 0
        BEGIN
            SELECT	{6}
            FROM	@modifiedRecordsTable
            WHERE	[RowNumber] = @rowsToProcess

            IF @dmlType = '{10}'
            BEGIN
                {7}
            END

            IF @dmlType = '{11}'
            BEGIN
                {8}
            END

            IF @dmlType = '{12}'
            BEGIN
                {9}
            END

            SET @rowsToProcess = @rowsToProcess - 1
        END{15}
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000)
        DECLARE @ErrorSeverity INT
        DECLARE @ErrorState INT

        SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE()

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState) {16}
    END CATCH
END";

    public const string InsertInTableVariableConsideringUpdateOf = @"IF ({0})
        BEGIN
            SET @dmlType = '{1}'
            {2}
        END
        ELSE BEGIN
            RETURN;
        END";

    public const string InsertInTableVariable = @"SET @dmlType = '{0}';
            {1}";

    // {2} = broker schema (drives @schema_id, queues, activation proc); {5} = table schema (trigger lives on the table).
    public const string ScriptDropAll = @"DECLARE @conversation_handle UNIQUEIDENTIFIER;
        DECLARE @schema_id INT;
        SELECT @schema_id = schema_id FROM sys.schemas WITH (NOLOCK) WHERE name = N'{2}';

        PRINT N'SqlTableDependency: Dropping trigger [{5}].[tr_{0}_Sender].';
        IF EXISTS (SELECT * FROM sys.triggers WITH (NOLOCK) WHERE object_id = OBJECT_ID(N'[{5}].[tr_{0}_Sender]'))
        BEGIN
            SELECT 1;
            {3}
        END

        PRINT N'SqlTableDependency: Deactivating queue [{2}].[{0}_Sender].';
        IF EXISTS (SELECT * FROM sys.service_queues WITH (NOLOCK) WHERE schema_id = @schema_id AND name = N'{0}_Sender') EXEC (N'ALTER QUEUE [{2}].[{0}_Sender] WITH ACTIVATION (STATUS = OFF)');

        PRINT N'SqlTableDependency: Ending conversations {0}.';
        SELECT conversation_handle INTO #Conversations FROM sys.conversation_endpoints WITH (NOLOCK) WHERE far_service LIKE N'{0}_%' ORDER BY is_initiator ASC;
        DECLARE conversation_cursor CURSOR FAST_FORWARD FOR SELECT conversation_handle FROM #Conversations;
        OPEN conversation_cursor;
        FETCH NEXT FROM conversation_cursor INTO @conversation_handle;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            END CONVERSATION @conversation_handle WITH CLEANUP;
            FETCH NEXT FROM conversation_cursor INTO @conversation_handle;
        END
        CLOSE conversation_cursor;
        DEALLOCATE conversation_cursor;
        DROP TABLE #Conversations;

        PRINT N'SqlTableDependency: Dropping service broker {0}_Receiver.';
        IF EXISTS (SELECT * FROM sys.services WITH (NOLOCK) WHERE name = N'{0}_Receiver') DROP SERVICE [{0}_Receiver];
        PRINT N'SqlTableDependency: Dropping service broker {0}_Sender.';
        IF EXISTS (SELECT * FROM sys.services WITH (NOLOCK) WHERE name = N'{0}_Sender') DROP SERVICE [{0}_Sender];

        PRINT N'SqlTableDependency: Dropping queue {2}.[{0}_Receiver].';
        IF EXISTS (SELECT * FROM sys.service_queues WITH (NOLOCK) WHERE schema_id = @schema_id AND name = N'{0}_Receiver') DROP QUEUE [{2}].[{0}_Receiver];
        PRINT N'SqlTableDependency: Dropping queue {2}.[{0}_Sender].';
        IF EXISTS (SELECT * FROM sys.service_queues WITH (NOLOCK) WHERE schema_id = @schema_id AND name = N'{0}_Sender') DROP QUEUE [{2}].[{0}_Sender];

        PRINT N'SqlTableDependency: Dropping contract {0}.';
        IF EXISTS (SELECT * FROM sys.service_contracts WITH (NOLOCK) WHERE name = N'{0}') DROP CONTRACT [{0}];
        PRINT N'SqlTableDependency: Dropping messages.';
        {1}

        PRINT N'SqlTableDependency: Dropping activation procedure {0}_QueueActivationSender.';
        IF EXISTS (SELECT * FROM sys.objects WITH (NOLOCK) WHERE schema_id = @schema_id AND name = N'{0}_QueueActivationSender') DROP PROCEDURE [{2}].[{0}_QueueActivationSender];

        IF EXISTS (SELECT * FROM sys.triggers WITH (NOLOCK) WHERE object_id = OBJECT_ID(N'[{5}].[tr_{0}_Sender]'))
        BEGIN
            SELECT 1;
            {4}
        END";

    // DDL identifiers cannot be bound as parameters, so each name arrives as one and is QUOTENAME'd into dynamic SQL.
    // QUOTENAME yields NULL for a name over 128 characters, which sp_executesql would silently skip, hence the guard.
    private const string RaiseOnNullSql = @"
        IF @sql IS NULL
        BEGIN
            RAISERROR(N'SqlTableDependency: object name exceeds 128 characters.', 16, 1);
            RETURN;
        END
        EXEC sp_executesql @sql;";

    // @message = message type name.
    public const string CreateMessageType = @"IF NOT EXISTS (SELECT 1 FROM sys.service_message_types WITH (NOLOCK) WHERE name = @message)
    BEGIN
        DECLARE @sql NVARCHAR(MAX) = N'CREATE MESSAGE TYPE ' + QUOTENAME(@message) + N' VALIDATION = NONE;';" + RaiseOnNullSql + @"
    END";

    // @contract = contract name; @messages = <m><m>name</m>...</m>, one element per message type sent by the initiator.
    public const string CreateContract = @"IF NOT EXISTS (SELECT 1 FROM sys.service_contracts WITH (NOLOCK) WHERE name = @contract)
    BEGIN
        DECLARE @xml XML = @messages;
        DECLARE @body NVARCHAR(MAX) = STUFF((
            SELECT N', ' + QUOTENAME(t.m.value('.', 'sysname')) + N' SENT BY INITIATOR'
            FROM @xml.nodes('/m/m') AS t(m)
            FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)'), 1, 2, N'');
        DECLARE @sql NVARCHAR(MAX) = N'CREATE CONTRACT ' + QUOTENAME(@contract) + N' (' + @body + N');';" + RaiseOnNullSql + @"
    END";

    // @schema = broker schema; @queue = queue name.
    public const string CreateQueue = @"IF NOT EXISTS (SELECT 1 FROM sys.service_queues WITH (NOLOCK) WHERE schema_id = SCHEMA_ID(@schema) AND name = @queue)
    BEGIN
        DECLARE @sql NVARCHAR(MAX) = N'CREATE QUEUE ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@queue)
            + N' WITH STATUS = ON, RETENTION = OFF, POISON_MESSAGE_HANDLING (STATUS = OFF);';" + RaiseOnNullSql + @"
    END";

    // @service = service name; @authorization = owner or NULL; @schema/@queue = its queue; @contract = contract or NULL (initiator side).
    public const string CreateService = @"IF NOT EXISTS (SELECT 1 FROM sys.services WITH (NOLOCK) WHERE name = @service)
    BEGIN
        DECLARE @sql NVARCHAR(MAX) = N'CREATE SERVICE ' + QUOTENAME(@service)
            + CASE WHEN @authorization IS NULL THEN N'' ELSE N' AUTHORIZATION ' + QUOTENAME(@authorization) END
            + N' ON QUEUE ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@queue)
            + CASE WHEN @contract IS NULL THEN N'' ELSE N' (' + QUOTENAME(@contract) + N')' END + N';';" + RaiseOnNullSql + @"
    END";

    // @schema = broker schema; @queue = sender queue; @procedure = activation procedure; @executeAs = SELF, OWNER or a user name.
    // @executeAsIsQuoted distinguishes a principal literally named SELF or OWNER from those keywords.
    public const string ActivateQueue = @"DECLARE @sql NVARCHAR(MAX) = N'ALTER QUEUE ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@queue)
        + N' WITH ACTIVATION (PROCEDURE_NAME = ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@procedure)
        + N', MAX_QUEUE_READERS = 1, EXECUTE AS '
        + CASE WHEN @executeAsIsQuoted = 0 AND UPPER(@executeAs) IN (N'SELF', N'OWNER') THEN UPPER(@executeAs) ELSE QUOTENAME(@executeAs, N'''') END
        + N', STATUS = ON);';" + RaiseOnNullSql;

    // @sender/@receiver = services; @contract = contract name. Returns the new conversation handle.
    public const string BeginConversation = @"DECLARE @h UNIQUEIDENTIFIER;
        DECLARE @sql NVARCHAR(MAX) = N'BEGIN DIALOG CONVERSATION @h FROM SERVICE ' + QUOTENAME(@sender)
            + N' TO SERVICE @receiver ON CONTRACT ' + QUOTENAME(@contract) + N' WITH ENCRYPTION = OFF;';
        IF @sql IS NULL
        BEGIN
            RAISERROR(N'SqlTableDependency: object name exceeds 128 characters.', 16, 1);
            RETURN;
        END
        EXEC sp_executesql @sql, N'@h UNIQUEIDENTIFIER OUTPUT, @receiver NVARCHAR(256)', @h OUTPUT, @receiver;
        SELECT @h;";

    public const string BeginConversationTimer = "BEGIN CONVERSATION TIMER (@handle) TIMEOUT = @timeout;";

    // Repair: returns 0 without ALTER on the database, 1 once enabled. NO_WAIT fails at once rather than killing other sessions (never ROLLBACK IMMEDIATE).
    // When another database already runs an enabled broker with this one's ID (a restored copy), ENABLE_BROKER fails with 9772 and
    // NEW_BROKER is the only way back; it ends this database's conversations. The collision is detected up front because, inside
    // EXEC, the CATCH sees the trailing 5069 ("ALTER DATABASE statement failed"), not 9772. sys.databases is readable via VIEW ANY DATABASE (public).
    // Issued from master so this session does not itself count against the exclusive access the change needs.
    public const string EnableBroker = @"SET NOCOUNT ON;
IF ISNULL(HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'ALTER'), 0) <> 1 BEGIN SELECT 0; RETURN; END
DECLARE @db NVARCHAR(300) = QUOTENAME(DB_NAME());
DECLARE @option NVARCHAR(20) = CASE WHEN EXISTS (
    SELECT 1 FROM sys.databases WITH (NOLOCK)
    WHERE database_id <> DB_ID() AND is_broker_enabled = 1
      AND service_broker_guid = (SELECT service_broker_guid FROM sys.databases WITH (NOLOCK) WHERE database_id = DB_ID()))
    THEN N'NEW_BROKER' ELSE N'ENABLE_BROKER' END;
EXEC (N'USE [master]; ALTER DATABASE ' + @db + N' SET ' + @option + N' WITH NO_WAIT;');
SELECT 1;";

    // Repair: CreateQueue is IF NOT EXISTS, so a queue switched off (by hand or by poison handling) is switched back on here.
    public const string EnableQueue = @"IF EXISTS (SELECT 1 FROM sys.service_queues WITH (NOLOCK) WHERE schema_id = SCHEMA_ID(@schema) AND name = @queue AND (is_receive_enabled = 0 OR is_enqueue_enabled = 0))
    BEGIN
        DECLARE @sql NVARCHAR(MAX) = N'ALTER QUEUE ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@queue) + N' WITH STATUS = ON;';" + RaiseOnNullSql + @"
    END";

    // Repair: CREATE OR ALTER TRIGGER keeps a disabled trigger disabled.
    public const string EnableTrigger = @"IF EXISTS (SELECT 1 FROM sys.triggers WITH (NOLOCK) WHERE object_id = OBJECT_ID(QUOTENAME(@schema) + N'.' + QUOTENAME(@trigger)) AND is_disabled = 1)
    BEGIN
        DECLARE @sql NVARCHAR(MAX) = N'ENABLE TRIGGER ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@trigger) + N' ON ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@table) + N';';" + RaiseOnNullSql + @"
    END";

    // @handle = conversation handle; 1 while the dialog is still usable.
    public const string ConversationIsOpen = "SELECT CASE WHEN EXISTS (SELECT 1 FROM sys.conversation_endpoints WITH (NOLOCK) WHERE conversation_handle = @handle AND state_desc NOT IN ('CLOSED', 'ERROR')) THEN 1 ELSE 0 END;";

    // WITH CLEANUP posts nothing to the _Sender queue, so retiring a replaced dialog cannot fire the activation procedure's drop-all.
    public const string EndConversationWithCleanup = "IF EXISTS (SELECT 1 FROM sys.conversation_endpoints WITH (NOLOCK) WHERE conversation_handle = @handle) END CONVERSATION @handle WITH CLEANUP;";

    // Read-only health probe: one row per problem, Kind/Problem spelled as the DatabaseObjectKind/DatabaseObjectProblem names.
    // Catalog names are only compared with parameters, never concatenated or UNIONed, so the broker catalogs' BIN collation is harmless.
    // @handle is NULL in persisted mode: the trigger re-begins a dialog itself, so a missing one is not a fault there.
    public const string SelectDatabaseObjectIssues = @"SET NOCOUNT ON;
DECLARE @issues TABLE ([Kind] NVARCHAR(32), [Name] NVARCHAR(512), [Problem] NVARCHAR(16));
DECLARE @brokerSchemaId INT = SCHEMA_ID(@brokerSchema);

IF NOT EXISTS (SELECT 1 FROM sys.databases WITH (NOLOCK) WHERE database_id = DB_ID() AND is_broker_enabled = 1)
    INSERT INTO @issues VALUES (N'Broker', DB_NAME(), N'Disabled');

DECLARE @triggerDisabled BIT = (SELECT is_disabled FROM sys.triggers WITH (NOLOCK) WHERE object_id = OBJECT_ID(QUOTENAME(@tableSchema) + N'.' + QUOTENAME(@trigger)));
IF @triggerDisabled IS NULL INSERT INTO @issues VALUES (N'Trigger', @trigger, N'Missing');
ELSE IF @triggerDisabled = 1 INSERT INTO @issues VALUES (N'Trigger', @trigger, N'Disabled');

INSERT INTO @issues
SELECT N'Queue', q.[name], CASE WHEN sq.object_id IS NULL THEN N'Missing' ELSE N'Disabled' END
FROM (VALUES (@receiver), (@sender)) AS q([name])
LEFT JOIN sys.service_queues sq WITH (NOLOCK) ON sq.schema_id = @brokerSchemaId AND sq.name = q.[name]
WHERE sq.object_id IS NULL OR sq.is_receive_enabled = 0 OR sq.is_enqueue_enabled = 0;

IF EXISTS (SELECT 1 FROM sys.service_queues WITH (NOLOCK) WHERE schema_id = @brokerSchemaId AND name = @sender AND (is_activation_enabled = 0 OR activation_procedure IS NULL))
    INSERT INTO @issues VALUES (N'QueueActivation', @sender, N'Disabled');

INSERT INTO @issues
SELECT N'Service', s.[name], N'Missing'
FROM (VALUES (@receiver), (@sender)) AS s([name])
WHERE NOT EXISTS (SELECT 1 FROM sys.services WITH (NOLOCK) WHERE name = s.[name]);

IF NOT EXISTS (SELECT 1 FROM sys.service_contracts WITH (NOLOCK) WHERE name = @contract)
    INSERT INTO @issues VALUES (N'Contract', @contract, N'Missing');

DECLARE @xml XML = @messages;
INSERT INTO @issues
SELECT N'MessageType', t.m.value('.', 'sysname'), N'Missing'
FROM @xml.nodes('/m/m') AS t(m)
WHERE NOT EXISTS (SELECT 1 FROM sys.service_message_types WITH (NOLOCK) WHERE name = t.m.value('.', 'sysname'));

IF OBJECT_ID(QUOTENAME(@brokerSchema) + N'.' + QUOTENAME(@procedure), N'P') IS NULL
    INSERT INTO @issues VALUES (N'Procedure', @procedure, N'Missing');

IF @handle IS NOT NULL AND NOT EXISTS (SELECT 1 FROM sys.conversation_endpoints WITH (NOLOCK) WHERE conversation_handle = @handle AND state_desc NOT IN ('CLOSED', 'ERROR'))
    INSERT INTO @issues VALUES (N'Conversation', CONVERT(NVARCHAR(36), @handle), N'Missing');

SELECT [Kind], [Name], [Problem] FROM @issues;";
}