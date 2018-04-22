# Monitor and receive notifications on record table changes

[![license](https://img.shields.io/github/license/mashape/apistatus.svg)](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/LICENSE.txt) [![license](https://img.shields.io/badge/release-6.1.0-brightgreen.svg)](#) [![date](https://img.shields.io/badge/date-May%2026%2C%202017-orange.svg)](#) [![NuGet Badge](https://buildstats.info/nuget/SqlTableDependency)](https://www.nuget.org/packages/SqlTableDependency/) [![SQL Server](https://img.shields.io/badge/SQL%20Server-%3E%3D2012-RED.svg)](#) [![.NET](https://img.shields.io/badge/.NET-%3E%3D%204.5.1-ff69b4.svg)](#)

**SqlTableDependency** is a high-level C# component used to audit, monitor and receive notifications on SQL Server's record table changes.

For any record table change, as insert, update or delete operation, a notification **containing values for the record changed** is received from SqlTableDependency. This notification contains the update values from the database table.

<img src="https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/Workflow-min.png" />

This tracking change system has the advantage to avoid a database select to retrieve updated table record, because the updated table values record is delivered to you by notification.

## Track record table changes
If we want **get alert about record table changes** without paying attention to the underlying SQL Server infrastructure then SqlTableDependency's record table change notifications will do that for us. Using notifications, an application can **detect table record changes** saving us from having to continuously re-query the database to get new values: for any record change, SqlTableDependency's event handler will get a notification containing modified table record values as well as the INSERT, UPDATE, DELETE operation type executed on our table.

Assuming we are interested to receive record changes for the following database table:

<img src="https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/rsz_table.jpg" />

Start installing SqlTableDependency using:

[![Install-Package SqlTableDependency](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/NuGetSqlTableDependency.png)](https://www.nuget.org/packages/SqlTableDependency/)

We define a C# model object mapping table columns we are interested to be populated with the values from any INSERT, DELETE or UPDATE operation. We do not need to define all table columns but just the ones we are interested in:

```C#
public class Customer
{
    public int Id { get; set; }
    public string Name { get; set; }
    public string Surname { get; set; }
}
```
Properties can have a different name from table column. We'll see later how to establish a mapping.

Create the SqlTableDependency object passing the connection string and table name (only necessary if the C# model name is different from the table name). Then create an event handler for SqlTableDependency's Changed event:

```C#
using TableDependency;
using TableDependency.SqlClient;
using TableDependency.EventArgs;

public class Program
{
   private static string _con= "data source=.; initial catalog=MyDB; integrated security=True";
   
   public static void Main()
   {
       // The mappar is use to link model properties with table columns name in case name do not match
       var mapper = new ModelToTableMapper<Customer>();
       mapper.AddMapping(c => c.Surname, "Second Name");
       mapper.AddMapping(c => c.Name, "First Name");

       // Here - as second parameter - we pass table name: this is necessary because the model name is 
       // different from table name (Customer vs Customers)
       using (var dep = new SqlTableDependency<Customer>(_con, "Customers", mapper))
       {
           dep.OnChanged += Changed;
           dep.Start();

           Console.WriteLine("Press a key to exit");
           Console.ReadKey();

           dep.Stop();
        }
   }

   public static void Changed(object sender, RecordChangedEventArgs<Customer> e)
   {
      var changedEntity = e.Entity;
      Console.WriteLine("DML operation: " + e.ChangeType);
      Console.WriteLine("ID: " + changedEntity.Id);
      Console.WriteLine("Name: " + changedEntity.Name);
      Console.WriteLine("Surame: " + changedEntity.Surname);
   }
}
```

Done! Now you are ready to receive notifications:

[![Receive SQL server notifications GIF video](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/Receive_notifications_from_Sql_Server_database.gif)](https://www.youtube.com/watch?v=sHJVusS5Qz0)

### Monitor table changes use cases and examples
To see SqlTableDependency in action, check the following [online long running test](http://sqltabledependency.somee.com/test).

Also, here are some examples of applications getting notification on record table change. After downloading the example, please remember to update SqlTableDependency nuget package:

* [Monitor table change with WPF and WCF](https://github.com/christiandelbianco/Monitor-table-change-with-WPF-WCF-sqltabledependency): This example shows how to continuously update a _DataGrid_ of stock data. The grid will be updated whenever a record change occurs. The notification event contains new values for the modified table record.
* [Monitor table change with MVC, SignalR and jQuery](https://github.com/christiandelbianco/monitor-table-change-with-mvc-signalR-jquery-sqltabledependency): This example shows how to continuously update a table containing stock data. The table will be updated whenever a record change occurs. The notification event contains new values for the modified table record.
* [Monitor table change with MVC, SignalR and Knockout JS](https://github.com/christiandelbianco/monitor-table-change-with-mvc-signalR-knockoutjs-sqltabledependency): This example shows how to refresh client web browsers used to book flight tickets. Those terminals have to be update as soon as the availability change and the Web application must take the initiative of sending this information to clients instead of waiting for the client to request it. 

This section reports some use case examples. Some of these examples, use the OracleTableDependency that is not longer supported. However, the example is still valid for SqlTableDependency:

* [Model and properties with same name of table and columns.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Model-and-properties-with-same-name-of-table-and-columns)
* [Code First Data Annotations to map model with database table.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Code-First-Data-Annotations-to-map-model-with-database-table)
* [Explicit database table name.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Explicit-database-table-name)
* [Custom map between model property and table column using ModelToTableMapper<T>.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Custom-map-between-model-property-and-table-column-using-ModelToTableMapper-T-)
* [Specify for which properties we want receive notification using UpdateOfModel<T> mapper.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Specify-for-which-properties-we-want-receive-notification-using-UpdateOfModel-T--mapper)
* [Filter notification by operation type.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Filter-notification-by-operation-type)
* [Get Errors.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Get-errors)
* [Logging.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Logging)
* [Get Status.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Status-change)
* [Apply filter based on WHERE condition.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Where-filter)

#### Remark
The `Start(int timeOut = 120, int watchDogTimeOut = 180)` method starts the listener to receive record change notifications.
The `watchDogTimeOut` parameter specifies the amount of time in seconds for the watch dog system.

After calling the `Stop()` method, notifications are not longer delivered. Database objects created by SqlTableDependency will be deleted.

It is a good practice - when possible - wrap SqlTableDependency within a using statement or alternatively in a try catch block: when the application will stop, this is enough to remove the SqlTableDependency infrastructure (Trigger, Service Broker service, the queue, Contract, Messages type and Stored Procedure) automatically.

However, when the application exits abruptly – that is by not calling the `Stop()` and/or `Dispose()` method - we need a way to cleaning up the SqlTableDependency infrastructure. The `Start()` method takes an optional parameter `watchDogTimeOut`. If there are no listeners waiting for notifications, the SqlTableDependency infrastructure will be removed after this period of time. The default value of `watchDogTimeOut` is 180 seconds.

**Notice:** There is a common scenario that could trigger the watchdog: _debugging_. During development, you often spend several minutes inside the debugger before you move on to the next step. Please make sure to increase `watchDogTimeOut` when you debug an application, otherwise you will experience an unexpected destruction of database objects in the middle of your debugging activity.

#### Under The Hood
SqlTableDependency's record change audit, provides the low-level implementation to receive database notifications creating SQL Server triggers, queues and service broker that immediately notifies your application when a record table change happens.

Assuming we want to monitor the \[dbo.Customer\] table content, we create a SqlTableDependency object specifying the Customer table and the following database objects will be generated:
* Message types
* Contract
* Queue
* Service Broker
* Trigger on table to be monitored
* Stored procedure to clean up the created objects in case the application exits abruptly (that is, when the application terminate without disposing the SqlTableDependency object)

![DatabaseObjects][DatabaseObjects]

[DatabaseObjects]: https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/DbObjects-min.png "Database Object created for send notifications"

### ![alt text](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/docs/if_exclamation-red_46014.png) Requirements 
* SQL Server 2008 R2 or latest versions (**please see note about Compatibility Level and Database Version**).
* .NET Framewrok 4.5.1 or latest versions.
* Server/windows service hosting SqlTableDependency **must not goes to SLEEP mode or idle state**. Sleep mode blocks SqlTableDependency code and this result in running the database watch dog that drops all SqlTableDependency's db objects.
* Backup and Restore of database already containing SqlTableDependency's db objects, does not work.

When you use notifications, you must be sure to enable Service Broker for the database. To do so, please run the following command:
```SQL
ALTER DATABASE MyDatabase SET ENABLE_BROKER
```
In case the user specified in the connection string is not database operator and neither has *db_owner* role, please make sure to GRANT the following permissions:
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

It is possible skip permissions test done by SqlTableDependency setting `executeUserPermissionCheck` constructor parameter to `false`. Otherwise an SQL server exception will be thrown if user does not have sufficient permissions.

### ![alt text](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/docs/if_exclamation-red_46014.png) Note about Compatibility Level and Database Version
From time to time, I receive bugs reporting issue like "not detect any record are changed". One of the possible cause of this missing record change notification, is due to Database compatibility version. Even if your SQL Server instance is SQL Server 2008 R2 or latest versions, can be that Database you are using was created using an old SQL Server version, for example SQL Server 2005.
To reproduce this issue infact, I download Northwind.mdf file and then I attached to my SQL Server 2008 R2 instance. Running SqlTableDependency against it, no exception is raised as well as no notification on record change is detected.

In order to discover your database compatibility version, you can use the following SQL script (see details on http://jongurgul.com/blog/database-created-version-internal-database-version-dbi_createversion/). Executing this script on my Northwind database I get:

```SQL
USE Northwind

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
```
<img src="https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/docs/2018-04-20%20at%2010-40-04.png" />

Executing same script on DB created by SQL Server 2008 R2 instance (TableDependencyDB), the result is:

<img src="https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/docs/2018-04-20%20at%2011-51-49.png" />

Even if your SQL Server instance is 2008 R2 or greater, DB compatibility level (VALUE column) is fundamental to receive record change notifications!

### Not supported SQL Server types
Following SQL Server columns types are not supported by SqlTableDepdency:
* XML
* IMAGE
* TEXT/NTEXT
* STRUCTURED
* GEOGRAPHY
* GEOMETRY
* HIERARCHYID
* SQL_VARIANT

#### Useful link
* https://sqlrus.com/2014/10/compatibility-level-vs-database-version/
* https://stackoverflow.com/questions/41169144/sqltabledependency-onchange-event-not-fired
* https://stackoverflow.com/questions/11383145/sql-server-2008-service-broker-tutorial-cannot-receive-the-message-exception

#### Contributors
Please, feel free to help and contribute with this project adding your comments, issues or bugs found as well as proposing fix and enhancements. [See contributors](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Contributors).

