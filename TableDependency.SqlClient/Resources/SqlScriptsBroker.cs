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

namespace TableDependency.SqlClient.Resources
{
    public static partial class SqlScripts
    {
        public const string CreateProcedureQueueActivation = @"CREATE PROCEDURE [{2}].[{0}_QueueActivationSender] AS 
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

        public const string CreateTrigger = @"CREATE TRIGGER [tr_{0}_Sender] ON {1} AFTER {13} AS 
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

    DECLARE @conversationHandlerExists INT
    SELECT @conversationHandlerExists = COUNT(*) FROM sys.conversation_endpoints WHERE conversation_handle = '{19}';
    IF @conversationHandlerExists = 0
    BEGIN
        DROP TRIGGER [tr_{0}_Sender];
        RETURN
    END
    
    IF NOT EXISTS(SELECT 1 FROM INSERTED)
    BEGIN
        SET @dmlType = '{12}'
        INSERT INTO @modifiedRecordsTable SELECT {3} FROM DELETED {14}
    END
    ELSE
    BEGIN
        IF NOT EXISTS(SELECT * FROM DELETED)
        BEGIN
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

        public const string ScriptDropAll = @"DECLARE @conversation_handle UNIQUEIDENTIFIER;
        DECLARE @schema_id INT;
        SELECT @schema_id = schema_id FROM sys.schemas WITH (NOLOCK) WHERE name = N'{2}';

        PRINT N'SqlTableDependency: Dropping trigger [{2}].[tr_{0}_Sender].';
        IF EXISTS (SELECT * FROM sys.triggers WITH (NOLOCK) WHERE object_id = OBJECT_ID(N'[{2}].[tr_{0}_Sender]')) DROP TRIGGER [{2}].[tr_{0}_Sender];

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
        IF EXISTS (SELECT * FROM sys.objects WITH (NOLOCK) WHERE schema_id = @schema_id AND name = N'{0}_QueueActivationSender') DROP PROCEDURE [{2}].[{0}_QueueActivationSender];";
    }
}