<meta name='keywords' content='Notification, monitor, record, table change, SQL Server'>
<meta name='description' content='Monitor SQL Server table and receive notifications for any record table change as insert, update or delete change'>
<meta name='subject' content='SQL Server monitor record table changes'>
<!--
  Title: Get SQL Server notifications when record table change
  Description: Receive notification for all insert, update or delete on SQL Server table change
  Author: christiandelbianco
  -->
  
# Monitor and receive notifications on record table changes

[![License](https://img.shields.io/cocoapods/l/AFNetworking.svg)](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/LICENSE.txt) [![Release](https://img.shields.io/badge/release-7.3.0-brightgreen.svg)](#) [![Updated](https://img.shields.io/badge/date-May%2021%2C%202018-orange.svg)](#) [![NuGet Badge](https://buildstats.info/nuget/SqlTableDependency)](https://www.nuget.org/packages/SqlTableDependency/) [![SQL Server](https://img.shields.io/badge/SQL%20Server-%3E%3D2008R2-RED.svg)](#) [![.NET](https://img.shields.io/badge/.NET-%3E%3D%204.5.1-ff69b4.svg)](#)

**SqlTableDependency** is a high-level C# component used to audit, monitor and receive notifications on SQL Server's record table changes. For any record table change, as insert, update or delete operation, a notification **containing values for the record changed** is is delivered to SqlTableDependency. This notification contains the insert, update or delete values from the database table.

<img src="https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/img/Workflow-min.png" />

This **table record tracking change** system has the advantage to avoid a select to retrieve updated table record, because the updated table values record is delivered by notification.

## Track record table changes
If we want **get alert on record table change** without paying attention to the underlying SQL Server infrastructure then SqlTableDependency's record table change notifications will do that for us. Using notifications, an application can **detect table record change** saving us from having to continuously re-query the database to get new values: for any record table change, SqlTableDependency's event handler get a notification containing modified table record values as well as the INSERT, UPDATE, DELETE operation type executed on database table.

As example, let's assume we are interested to receive record table changes for the following database table:

<img src="https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/img/rsz_table.jpg" />

after have installed SqlTableDependency using:

[![Install-Package SqlTableDependency](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/img/NuGetSqlTableDependency.png)](https://www.nuget.org/packages/SqlTableDependency/)

we start defining a C# model mapping table columns we are interested: these properties will be populated with the values resulting from any INSERT, DELETE or UPDATE record table change operation. We do not need to specify all table columns but just the ones we are interested:

```C#
public class Customer
{
    public int Id { get; set; }
    public string Name { get; set; }
    public string Surname { get; set; }
}
```
Model properties can have different name from table columns. We'll see later how to establish a mapping between model properties and table columns with different name.

Now create SqlTableDependency instance passing the connection string and table name (database table name is necessary only if the C# model has a name that is different from the database table name). Then create an event handler for SqlTableDependency's Changed event:

```C#
using TableDependency;
using TableDependency.SqlClient;
using TableDependency.EventArgs;

public class Program
{
   private static string _con= "data source=.; initial catalog=MyDB; integrated security=True";
   
   public static void Main()
   {
       // The mapper object is used to map model properties that do not have a corresponding table column name.
       // In case all properties of your model have same name of table columns, you can avoid to use the mapper.
       var mapper = new ModelToTableMapper<Customer>();
       mapper.AddMapping(c => c.Surname, "Second Name");
       mapper.AddMapping(c => c.Name, "First Name");

       // Here - as second parameter - we pass table name: this is necessary only if the model name is 
       // different from table name (in our case we have Customer vs Customers). 
       // If needed, you can also specifiy schema name.
       using (var dep = new SqlTableDependency<Customer>(_con, tableName: "Customers", mapper: mapper))
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

Done! Now you are ready to receive record table change notifications:

[![Receive SQL server notifications GIF video](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/img/Receive_notifications_from_Sql_Server_database.gif)](https://www.youtube.com/watch?v=sHJVusS5Qz0)

### Monitor table changes use cases and examples
To see SqlTableDependency in action, check the following [online long running test](http://sqltabledependency.somee.com/test). Here, SqlTableDependency is tested continuously using a thread that every five seconds perform an update record table change. SqlTableDependency monitor this record table change and get a notification containing new update record table values.  

Also, here are some examples of applications getting notification on record table change. After downloading the example, please remember to update SqlTableDependency nuget package:

* [Monitor table change with WPF and WCF](https://github.com/christiandelbianco/Monitor-table-change-with-WPF-WCF-sqltabledependency): This example shows how to refresh a _DataGrid_ of stock data. The grid will be updated whenever a record table change occurs. The notification event contains new values for the modified table record.
* [Monitor table change with MVC, SignalR and jQuery](https://github.com/christiandelbianco/monitor-table-change-with-mvc-signalR-jquery-sqltabledependency): This example shows how to refresh a HTML table containing stock values. The HTML table will be updated whenever a record table change occurs. Notification event contains new values for the modified table record.
* [Monitor table change with MVC, SignalR and Knockout JS](https://github.com/christiandelbianco/monitor-table-change-with-mvc-signalR-knockoutjs-sqltabledependency): This example shows how to refresh client web browsers used to book flight tickets. Those terminals must be updated as soon as the availability change and the Web application must take the initiative of sending this information to clients instead of waiting for the client to request it. 

This section reports some use case examples:

* [Model and properties with same name of table and columns.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Model-and-properties-with-same-name-of-table-and-columns)
* [Code First Data Annotations to map model with database table.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Code-First-Data-Annotations-to-map-model-with-database-table)
* [Explicit database table name.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Explicit-database-table-name)
* [Custom map between model property and table column using ModelToTableMapper<T>.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Custom-map-between-model-property-and-table-column-using-ModelToTableMapper-T)
* [Specify for which properties we want receive notification using UpdateOfModel<T> mapper.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Specify-for-which-properties-we-want-receive-notification-using-UpdateOfModel-T--mapper)
* [Filter notification by change type.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Filter-notification-by-operation-type)
* [Get Errors.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Get-errors)
* [Logging.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Logging)
* [Get Status.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Status-change)
* [Apply filter based on WHERE condition.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Where-filter)
* [Include old values.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Include-old-values)

#### Remark
The `Start(int timeOut = 120, int watchDogTimeOut = 180)` method starts the listener to receive record table change notifications.
The `watchDogTimeOut` parameter specifies the amount of time in seconds for the watch dog system.

After calling the `Stop()` method, record table change notifications are not longer delivered. Database objects created by SqlTableDependency will be deleted.

It is a good practice - when possible - wrap SqlTableDependency within a using statement or alternatively in a try catch block: when the application will stop, this is enough to remove the SqlTableDependency infrastructure (Trigger, Service Broker service, the queue, Contract, Messages type and Stored Procedure) automatically.

In any case, when the application exits abruptly – that is by not calling the `Stop()` and/or `Dispose()` method - we need a way to cleaning up the SqlTableDependency infrastructure. The `Start()` method takes an optional parameter `watchDogTimeOut`. If there are no listeners waiting for notifications, the SqlTableDependency infrastructure will be removed after this period of time. The default value of `watchDogTimeOut` is 180 seconds.

There is a common scenario that could trigger the watchdog: debugging. During development, you often spend several minutes inside the debugger before you move on to the next step. Please make sure to increase `watchDogTimeOut` when you debug an application, otherwise you will experience an unexpected destruction of database objects in the middle of your debugging activity.

#### Under The Hood
SqlTableDependency's record change audit, provides the low-level implementation to receive database record table change notifications creating SQL Server triggers, queues and service broker that immediately notifies your application when a record table change happens.

Assuming we want to monitor the \[dbo.Customer\] table content, we create a SqlTableDependency object specifying the Customer table and the following database objects will be generated:
* Message types
* Contract
* Queue
* Service Broker
* Trigger on table to be monitored
* Stored procedure to clean up the created objects in case the application exits abruptly (that is, when the application terminate without disposing the SqlTableDependency object)

![DatabaseObjects][DatabaseObjects]

[DatabaseObjects]: https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/img/DbObjects-min.png "Database Object created for send notifications"

#### ![alt text](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/img/if_exclamation-red_46014.png) Requirements 
* SQL Server 2008 R2 or latest versions (**please see note about Compatibility Level and Database Version**).
* .NET Framewrok 4.5.1 or latest versions.
* Windows service using SqlTableDependency **must not goes to SLEEP mode or IDLE state**. Sleep mode blocks SqlTableDependency code and this result in running the database watch dog that drops all SqlTableDependency's db objects (please see https://stackoverflow.com/questions/6302185/how-to-prevent-windows-from-entering-idle-state).
* Database Backup and Restore: restoring SqlTableDependency's db objects, it does **not work**.

To use notifications, you must be sure to enable Service Broker for the database. To do this run the SQL command:
```SQL
ALTER DATABASE MyDatabase SET ENABLE_BROKER
```
In case the user specified in the connection string is not database **Administrator**, **db owner** or neither has **db_owner** role, please make sure to GRANT the following permissions to your login user:
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

It is possible skip permissions test done by SqlTableDependency setting `executeUserPermissionCheck` constructor parameter to `false`. Nevertheless a SQL server exception will be thrown if user have not sufficient permissions.

#### ![alt text](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/img/if_exclamation-red_46014.png) Note about Compatibility Level and Database Version
From time to time, I receive bugs reporting issue like "I not detect/receive any record table change notification". Assuming that you are using a logic with enough grants, one of the possible cause of this missing table record change notification, is due to Database compatibility version. Even if your SQL Server instance is SQL Server 2008 R2 or latest versions, can be that your Databasehas been created using an old SQL Server version, for example SQL Server 2005.
To reproduce this issue, you can download Northwind.mdf file and then attach it to your SQL Server 2008 R2 (or greater) instance. Running SqlTableDependency against it, no exception is raised as well as no notification on record table change is detected.

In order to discover your database compatibility version, you can use the following SQL script (see details on http://jongurgul.com/blog/database-created-version-internal-database-version-dbi_createversion/). 

```SQL
USE <your db>

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
Executing this script on Northwind database you get:

<img src="https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/img/2018-04-20%20at%2010-40-04.png" />

Executing this script on DB created by SQL Server 2008 R2 instance (database name TableDependencyDB), the result is:

<img src="https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/img/2018-04-20%20at%2011-51-49.png" />

So, even if your SQL Server instance is 2008 R2 or greater, DB compatibility level (VALUE column) is fundamental to receive record table change notifications.

#### Not supported SQL Server table column types
Following SQL Server columns types are **not** supported by SqlTableDepdency:
* XML
* IMAGE
* TEXT/NTEXT
* STRUCTURED
* GEOGRAPHY
* GEOMETRY
* HIERARCHYID
* SQL_VARIANT

#### Useful link and tips
* https://sqlrus.com/2014/10/compatibility-level-vs-database-version/
* https://stackoverflow.com/questions/41169144/sqltabledependency-onchange-event-not-fired
* https://stackoverflow.com/questions/11383145/sql-server-2008-service-broker-tutorial-cannot-receive-the-message-exception
* Deleting multiple records then inserting using **sql bulk copy**, only deleted change events are raised: to solve this problem, set **SqlBulkCopyOptions.FireTriggers**. Thanks to Ashraf Ghorabi! 

#### Contributors
Please, feel free to help and contribute with this project adding your comments, issues or bugs found as well as proposing fix and enhancements. [See contributors](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Contributors).

