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

namespace TableDependency.SqlClient.Resources
{
    public static class SqlScripts
    {
        public const string CreateProcedureQueueActivation = @"
CREATE PROCEDURE {2}.[{0}_QueueActivation] AS 
BEGIN 
    SET NOCOUNT ON;
    DECLARE @h AS UNIQUEIDENTIFIER;

    PRINT N'SqlTableDependency: Queue activation stored procedure {0} has been invoked.';
    RECEIVE TOP(0) @h = [conversation_handle] FROM {2}.[{0}];

    IF EXISTS (SELECT * FROM sys.service_queues WITH(NOLOCK) WHERE name = N'{0}')
    BEGIN
        IF ((SELECT COUNT(*) FROM {2}.[{0}] WITH(NOLOCK) WHERE message_type_name = N'http://schemas.microsoft.com/SQL/ServiceBroker/DialogTimer' OR message_type_name = N'{3}') > 0)
        BEGIN 
            PRINT N'SqlTableDependency: Drop objects {0} started.';
            {1}
            PRINT N'SqlTableDependency: Drop objects {0} ended.';
        END 
    END
END";

        public const string CreateTrigger = @"
CREATE TRIGGER [tr_{0}] ON {1} AFTER {13} AS 
BEGIN
    SET NOCOUNT ON;

    DECLARE @rowsToProcess INT
    DECLARE @currentRow INT
    DECLARE @h AS UNIQUEIDENTIFIER
    DECLARE @records XML
    DECLARE @theMessageContainer NVARCHAR(MAX)
    DECLARE @dmlType NVARCHAR(10)
    DECLARE @modifiedRecordsTable TABLE ([RowNumber] INT IDENTITY(1, 1), {2})
    {5}
    
    IF NOT EXISTS(SELECT * FROM INSERTED)
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

    SELECT @rowsToProcess = COUNT(*) FROM @modifiedRecordsTable    
    IF @rowsToProcess < 1 RETURN
    SET @currentRow = 0

    BEGIN TRY
        WHILE @currentRow < @rowsToProcess
        BEGIN
            SET @currentRow = @currentRow + 1

            SELECT	{6}
            FROM	@modifiedRecordsTable
            WHERE	[RowNumber] = @currentRow
                
            IF @dmlType = '{10}' 
            BEGIN
                BEGIN DIALOG CONVERSATION @h
                FROM SERVICE [{0}] TO SERVICE '{0}', 'CURRENT DATABASE' ON CONTRACT [{0}]
                WITH RELATED_CONVERSATION_GROUP = NEWID(), ENCRYPTION = OFF;

                {7}

                END CONVERSATION @h;
            END
        
            IF @dmlType = '{11}'
            BEGIN
                BEGIN DIALOG CONVERSATION @h
                FROM SERVICE [{0}] TO SERVICE '{0}', 'CURRENT DATABASE' ON CONTRACT [{0}]
                WITH RELATED_CONVERSATION_GROUP = NEWID(), ENCRYPTION = OFF;

                {8}

                END CONVERSATION @h;
            END

            IF @dmlType = '{12}'
            BEGIN
                BEGIN DIALOG CONVERSATION @h
                FROM SERVICE [{0}] TO SERVICE '{0}', 'CURRENT DATABASE' ON CONTRACT [{0}]
                WITH RELATED_CONVERSATION_GROUP = NEWID(), ENCRYPTION = OFF;

                {9}

                END CONVERSATION @h;
            END            
        END                       

        {15}
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000)
        DECLARE @ErrorSeverity INT
        DECLARE @ErrorState INT

        SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE()

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState) {16};
    END CATCH
END";

        public const string DisposeMessage = @"
DECLARE @conversation uniqueidentifier;
BEGIN DIALOG @conversation FROM SERVICE [{0}] TO SERVICE '{0}' ON CONTRACT [{0}] WITH ENCRYPTION=OFF;
SEND ON CONVERSATION @conversation MESSAGE TYPE [{1}] (0x);";

        public const string InformationSchemaColumns = @"
SELECT DB_NAME() AS TABLE_CATALOG,
SCHEMA_NAME(o.schema_id) AS TABLE_SCHEMA,
o.name AS TABLE_NAME,
c.name AS COLUMN_NAME,
COLUMNPROPERTY(c.object_id, c.name, 'ordinal') AS ORDINAL_POSITION,
convert(nvarchar(4000), OBJECT_DEFINITION(c.default_object_id))	AS COLUMN_DEFAULT,
convert(varchar(3), CASE c.is_nullable WHEN 1 THEN 'YES' ELSE 'NO' END)	AS IS_NULLABLE,
ISNULL(TYPE_NAME(c.system_type_id), t.name)	AS DATA_TYPE,
COLUMNPROPERTY(c.object_id, c.name, 'charmaxlen') AS CHARACTER_MAXIMUM_LENGTH,
COLUMNPROPERTY(c.object_id, c.name, 'octetmaxlen') AS CHARACTER_OCTET_LENGTH,
convert(tinyint, CASE -- int/decimal/numeric/real/float/money
WHEN c.system_type_id IN (48, 52, 56, 59, 60, 62, 106, 108, 122, 127) THEN c.precision
END) AS NUMERIC_PRECISION,
convert(smallint, CASE	-- int/money/decimal/numeric
WHEN c.system_type_id IN (48, 52, 56, 60, 106, 108, 122, 127) THEN 10
WHEN c.system_type_id IN (59, 62) THEN 2 END)	AS NUMERIC_PRECISION_RADIX,	-- real/float
convert(int, CASE	-- datetime/smalldatetime
WHEN c.system_type_id IN (40, 41, 42, 43, 58, 61) THEN NULL
ELSE ODBCSCALE(c.system_type_id, c.scale) END)	AS NUMERIC_SCALE,
convert(smallint, CASE -- datetime/smalldatetime
WHEN c.system_type_id IN (40, 41, 42, 43, 58, 61) THEN ODBCSCALE(c.system_type_id, c.scale) END)	AS DATETIME_PRECISION,
convert(sysname, null)	AS CHARACTER_SET_CATALOG,
convert(sysname, CASE WHEN c.system_type_id IN (35, 167, 175)	-- char/varchar/text
THEN COLLATIONPROPERTY(c.collation_name, 'sqlcharsetname')
WHEN c.system_type_id IN (99, 231, 239)	-- nchar/nvarchar/ntext
THEN N'UNICODE' END) AS CHARACTER_SET_NAME,
convert(sysname, null) AS COLLATION_CATALOG,
c.collation_name AS COLLATION_NAME,
convert(sysname, CASE WHEN c.user_type_id > 256
THEN DB_NAME() END)	AS DOMAIN_CATALOG, convert(sysname, CASE WHEN c.user_type_id > 256
THEN SCHEMA_NAME(t.schema_id)
END) AS DOMAIN_SCHEMA, convert(sysname, CASE WHEN c.user_type_id > 256  
THEN TYPE_NAME(c.user_type_id)
END) AS DOMAIN_NAME
FROM sys.objects o JOIN sys.columns c ON c.object_id = o.object_id
LEFT JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE o.type IN ('U') and SCHEMA_NAME(o.schema_id) = '{0}' and o.name = '{1}'";

        public const string InformationSchemaTables = @"
SELECT COUNT(*) FROM sys.objects o LEFT JOIN sys.schemas s ON s.schema_id = o.schema_id WHERE o.type IN ('U', 'V') and o.name = '{0}' and s.name = '{1}'";

        public const string ScriptDropAll = @"
DECLARE @schema_id INT;
DECLARE @conversation_handle UNIQUEIDENTIFIER;

SELECT @schema_id = schema_id FROM sys.schemas WITH (NOLOCK) WHERE name = N'{2}';

PRINT N'SqlTableDependency: Dropping trigger [{2}].[tr_{0}].'; 
IF EXISTS (SELECT * FROM sys.objects WITH (NOLOCK) WHERE schema_id = @schema_id AND name = N'tr_{0}') DROP TRIGGER [{2}].[tr_{0}];
PRINT N'SqlTableDependency: Deactivating queue [{2}].[{0}].';
IF EXISTS (SELECT * FROM sys.service_queues WITH(NOLOCK) WHERE schema_id = @schema_id AND name = N'{0}') EXEC (N'ALTER QUEUE [{2}].[{0}] WITH ACTIVATION (STATUS = OFF)');

PRINT N'SqlTableDependency: Ending conversations {0}.';
SELECT conversation_handle INTO #Conversations FROM sys.conversation_endpoints WITH (NOLOCK) WHERE far_service = N'{0}';
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

PRINT N'SqlTableDependency: Dropping service broker {0}.';
IF EXISTS (SELECT * FROM sys.services WITH (NOLOCK) WHERE name = N'{0}') DROP SERVICE [{0}];
PRINT N'SqlTableDependency: Dropping queue {2}.[{0}].';
IF EXISTS (SELECT * FROM sys.service_queues WITH (NOLOCK) WHERE schema_id = @schema_id AND name = N'{0}') DROP QUEUE {2}.[{0}];
PRINT N'SqlTableDependency: Dropping contract {0}.';
IF EXISTS (SELECT * FROM sys.service_contracts WITH (NOLOCK) WHERE name = N'{0}') DROP CONTRACT [{0}];
PRINT N'SqlTableDependency: Dropping messages.';
{1}

PRINT N'SqlTableDependency: Dropping activation procedure {0}_QueueActivation.';
IF EXISTS (SELECT * FROM sys.objects WITH (NOLOCK) WHERE schema_id = @schema_id AND name = N'{0}_QueueActivation') DROP PROCEDURE [{2}].[{0}_QueueActivation];";

        /// <summary>
        /// Security Audit Report
        /// 1) List all access provisioned to a sql user or windows user/group directly 
        /// 2) List all access provisioned to a sql user or windows user/group through a database or application role
        /// 3) List all access provisioned to the public role
        /// 
        /// Columns Returned
        /// UserName        : SQL or Windows/Active Directory user cccount.This could also be an Active Directory group.
        /// UserType        : Value will be either 'SQL User' or 'Windows User'.  This reflects the type of user defined for the SQL Server user account.
        /// DatabaseUserName: Name of the associated user as defined in the database user account.  The database user may not be the same as the server user.
        /// Role            : The role name.This will be null if the associated permissions to the object are defined at directly on the user account, otherwise this will be the name of the role that the user is a member of.
        /// PermissionType  : Type of permissions the user/role has on an object. Examples could include CONNECT, EXECUTE, SELECT DELETE, INSERT, ALTER, CONTROL, TAKE OWNERSHIP, VIEW DEFINITION, etc. This value may not be populated for all roles.  Some built in roles have implicit permission definitions.
        /// PermissionState : Reflects the state of the permission type, examples could include GRANT, DENY, etc. This value may not be populated for all roles.  Some built in roles have implicit permission definitions.
        /// ObjectType      : Type of object the user/role is assigned permissions on.Examples could include USER_TABLE, SQL_SCALAR_FUNCTION, SQL_INLINE_TABLE_VALUED_FUNCTION, SQL_STORED_PROCEDURE, VIEW, etc. This value may not be populated for all roles.  Some built in roles have implicit permission definitions.
        /// ObjectName      : Name of the object that the user/role is assigned permissions on. This value may not be populated for all roles.  Some built in roles have implicit permission definitions.
        /// ColumnName      : Name of the column of the object that the user/role is assigned permissions on.This value is only populated if the object is a table, view or a table value function.  
        /// </summary>
        public const string SelectUserGrants = @"
--List all access provisioned to a sql user or windows user/group directly 
SELECT  
    [UserName] = CASE princ.[type] WHEN 'S' THEN princ.[name] WHEN 'U' THEN ulogin.[name] COLLATE Latin1_General_CI_AI END,
    [UserType] = CASE princ.[type] WHEN 'S' THEN 'SQL User' WHEN 'U' THEN 'Windows User' END,  
    [DatabaseUserName] = princ.[name],       
    [Role] = null,      
    [PermissionType] = perm.[permission_name],       
    [PermissionState] = perm.[state_desc],       
    [ObjectType] = obj.type_desc,--perm.[class_desc],       
    [ObjectName] = OBJECT_NAME(perm.major_id),
    [ColumnName] = col.[name]
FROM    
    --database user
    sys.database_principals princ  
LEFT JOIN
    --Login accounts
    sys.login_token ulogin on princ.[sid] = ulogin.[sid]
LEFT JOIN        
    --Permissions
    sys.database_permissions perm ON perm.[grantee_principal_id] = princ.[principal_id]
LEFT JOIN
    --Table columns
    sys.columns col ON col.[object_id] = perm.major_id AND col.[column_id] = perm.[minor_id]
LEFT JOIN
    sys.objects obj ON perm.[major_id] = obj.[object_id]
WHERE 
    (princ.[type] in ('S','U') AND ulogin.sid = SUSER_SID(SUSER_SNAME())) OR (ulogin.sid in((select [sid] from [sys].[database_principals] where [type] in ('R','G'))))
UNION
--List all access provisioned to a sql user or windows user/group through a database or application role
SELECT  
    [UserName] = CASE memberprinc.[type] WHEN 'S' THEN memberprinc.[name] WHEN 'U' THEN ulogin.[name] COLLATE Latin1_General_CI_AI END,
    [UserType] = CASE memberprinc.[type] WHEN 'S' THEN 'SQL User' WHEN 'U' THEN 'Windows User' END, 
    [DatabaseUserName] = memberprinc.[name],   
    [Role] = roleprinc.[name],      
    [PermissionType] = perm.[permission_name],       
    [PermissionState] = perm.[state_desc],       
    [ObjectType] = obj.type_desc,--perm.[class_desc],   
    [ObjectName] = OBJECT_NAME(perm.major_id),
    [ColumnName] = col.[name]
FROM    
    --Role/member associations
    sys.database_role_members members
JOIN
    --Roles
    sys.database_principals roleprinc ON roleprinc.[principal_id] = members.[role_principal_id]
JOIN
    --Role members (database users)
    sys.database_principals memberprinc ON memberprinc.[principal_id] = members.[member_principal_id]
LEFT JOIN
    --Login accounts
    sys.login_token ulogin on memberprinc.[sid] = ulogin.[sid]
LEFT JOIN        
    --Permissions
    sys.database_permissions perm ON perm.[grantee_principal_id] = roleprinc.[principal_id]
LEFT JOIN
    --Table columns
    sys.columns col on col.[object_id] = perm.major_id 
                    AND col.[column_id] = perm.[minor_id]
LEFT JOIN
    sys.objects obj ON perm.[major_id] = obj.[object_id]
WHERE ulogin.sid = SUSER_SID(SUSER_SNAME()) OR ulogin.sid in((select [sid] from [sys].[database_principals] where [type] in ('R','G')))
UNION
--List all access provisioned to the public role, which everyone gets by default
SELECT  
    [UserName] = '{All Users}',
    [UserType] = '{All Users}', 
    [DatabaseUserName] = '{All Users}',       
    [Role] = roleprinc.[name],      
    [PermissionType] = perm.[permission_name],       
    [PermissionState] = perm.[state_desc],       
    [ObjectType] = obj.type_desc,--perm.[class_desc],  
    [ObjectName] = OBJECT_NAME(perm.major_id),
    [ColumnName] = col.[name]
FROM    
    --Roles
    sys.database_principals roleprinc
LEFT JOIN        
    --Role permissions
    sys.database_permissions perm ON perm.[grantee_principal_id] = roleprinc.[principal_id]
LEFT JOIN
    --Table columns
    sys.columns col on col.[object_id] = perm.major_id AND col.[column_id] = perm.[minor_id]                   
JOIN 
    --All objects   
    sys.objects obj ON obj.[object_id] = perm.[major_id]
WHERE
    --Only roles
    roleprinc.[type] = 'R' AND
    --Only public role
    roleprinc.[name] = 'public' AND
    --Only objects of ours, not the MS objects
    obj.is_ms_shipped = 0
ORDER BY
    princ.[Name],
    OBJECT_NAME(perm.major_id),
    col.[name],
    perm.[permission_name],
    perm.[state_desc],
    obj.type_desc--perm.[class_desc]";

        public const string TriggerUpdateWithColumns = @"IF ({0}) BEGIN
    SET @dmlType = '{3}'
    INSERT INTO @modifiedRecordsTable SELECT {2} FROM
    {4}
END
ELSE BEGIN
    RETURN
END";

        public const string TriggerUpdateWithoutColumns = @"
SET @dmlType = '{2}'
INSERT INTO @modifiedRecordsTable SELECT {1} FROM
{3}";

    }
}