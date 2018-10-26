-- https://sqlrus.com/2014/10/compatibility-level-vs-database-version/ 	
SELECT name, compatibility_level from sys.databases WHERE name in ('Northwind', 'TableDependencyDB')
GO

SELECT name, version from master.dbo.sysdatabases where name in ('Northwind', 'TableDependencyDB')
GO



DBCC TRACEON (3604)
DBCC PAGE('Northwind',1,9,3)
DBCC TRACEOFF (3604)
GO
--using DBCC DBINFO
DBCC TRACEON (3604)
DBCC DBINFO
DBCC TRACEOFF (3604)
GO
 
-- useing database property
SELECT DatabaseProperty('Northwind','version')
GODBCC TRACEON (3604)
DBCC PAGE('Northwind',1,9,3)
DBCC TRACEOFF (3604)
GO
--using DBCC DBINFO
DBCC TRACEON (3604)
DBCC DBINFO
DBCC TRACEOFF (3604)
GO
 
-- useing database property
SELECT DatabaseProperty('Northwind','version')
GO


-- *************************************************************

use Northwind

--http://jongurgul.com/blog/database-created-version-internal-database-version-dbi_createversion/
--http://connect.microsoft.com/SQLServer/feedback/details/640864/smo-property-database-version-does-not-return-the-version-used-to-create-the-database-but-the-version-that-is-has-been-upgraded-to-on-the-current-instance
DECLARE @DBINFO TABLE ([ParentObject] VARCHAR(60),[Object] VARCHAR(60),[Field] VARCHAR(30),[VALUE] VARCHAR(4000))
INSERT INTO @DBINFO
EXECUTE sp_executesql N'DBCC DBINFO WITH TABLERESULTS'
SELECT [Field]
,[VALUE]
,CASE
WHEN [VALUE] = 515 THEN 'SQL 7'
WHEN [VALUE] = 539 THEN 'SQL 2000'
WHEN [VALUE] IN (611,612) THEN 'SQL 2005'
WHEN [VALUE] = 655 THEN 'SQL 2008'
WHEN [VALUE] = 661 THEN 'SQL 2008R2'
WHEN [VALUE] = 706 THEN 'SQL 2012'
WHEN [VALUE] = 782 THEN 'SQL 2014'
WHEN [VALUE] = 852 THEN 'SQL 2016'
ELSE '?'
END [SQLVersion]
FROM @DBINFO
WHERE [Field] IN ('dbi_createversion','dbi_version')