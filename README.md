# Audit, monitor and receive notifications on table change from SQL Server

SqlTableDependency is a high-level C# component to used to audit, monitor and receive notifications on SQL Server's record table changes.

For any record table change, insert update or delete, a notification *containing values for the record **inserted**, **changed** or **deleted** is received from SqlTableDependency. This notification contains the update values int the database table.

Compared to Microsoft ADO.NET SqlDependency class, this tracking change system has the advantage of avoid a database select to retrieve updated table record state, because this latest table status is delivered by the received notification.

[![IMAGE ALT TEXT HERE](http://img.youtube.com/vi/FBkkdCuTO7g/0.jpg)](http://www.youtube.com/watch?v=FBkkdCuTO7g)

## Track record table change
If we want **get alert about record table changes** without paying attention to the underlying SQL Server infrastructure then SqlTableDependency's record table change notifications will do that for us. Using notifications, an application can **detect table record changes** saving us from having to continuously re-query the database to get new values.

SqlTableDependency's record change audit, provides the low-level implementation to receive database notifications creating SQL Server trigger, queue and service broker that immediately notify us when any record table changes happens.

For any record change, SqlTableDependency's event handler will get a notification containing modified table record values as well as the insert, update, delete operation type executed on our table.

Basically, it is an enhancement of .NET SqlDepenency with the advantage of send events containing values for the record inserted, changed or deleted, as well as the DML operation (insert/delete/update) executed on the table. This is the real difference with. NET SqlDepenency: this class, in fact, does not tell you what data was changed on the database.

### Under The Hood
Assuming we want monitor the Customer table contents, we create a SqlTableDependency object specifying the Customer table and the following database objects will be generated:
* Message types
* Contract
* Queue
* Service Broker
* Trigger on table to be monitored
* Stored procedure to clean up the created objects in case the application exits abruptly (that is, when the application terminate without disposing the SqlTableDependency object)

![alt text][DatabaseObjects]

[DatabaseObjects]: https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/DbObjects-min.png "Database Object created for send notifications"

## Requirements
When you use notifications, you must be sure to enable Service Broker for the database. To do that you can use the following command:
```SQL
ALTER DATABASE MyDatabase SET ENABLE_BROKER
```
Also case user specified in connection string is not DBO or has not db_owner role, he must have the following GRANT permissions:
* ALTER
* CONNECT
* CONTROL
* CREATE CONTRACT
* CREATE MESSAGE TYPE
* CREATE PROCEDURE
* CREATE QUEUE
* CREATE SERVICE
* EXECUTE
* SELECT
* SUBSCRIBE QUERY NOTIFICATIONS
* VIEW DATABASE STATE
* VIEW DEFINITION

## Few steps to get alert on table insert update delete
Let’s assume we are interested to receive record changes on the following database table:
```C#
CREATE TABLE [dbo].[Client](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[First Name] [nvarchar](50) NOT NULL,
	[Second Name] [nvarchar](50) NOT NULL,
	[Birthday] [datetime] NOT NULL,
	[DepartmentId] [int] NOT NULL)
```

1. Install SqlTableDependency using:

![alt text][Nuget]


[Nuget]: 
https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/NuGetSqlTableDependency.png "Nuget package"

2. Write your model defining interested properties:
```C#
public class Customers
{
    public int Id { get; set; }
    public string Name { get; set; }
    public string Surname { get; set; }
}
```
The model can avoid to define all table columns if you are not interested in some value.

More examples

    Monitor table change with WPF and WCF: This example show how to keep up to date a grid containing some stocks data. That grid has been automatically updated whenever a record change using database notifications. This notification contains new values for the modified table record.

    Monitor table change with MVC, SignalR and jQuery: This example show how to keep up to date a table containing some stocks data. That table has been automatically updated whenever a record change using database notifications. This notification contains new values for the modified table record.

    Monitor table change with MVC, SignalR and Knockout JS: This example show how to refresh client web browsers used to book flight tickets. Those terminals have to be update as soon as the availability change and the Web application must take the initiative of sending this information to clients instead of waiting for the client to request it. 
