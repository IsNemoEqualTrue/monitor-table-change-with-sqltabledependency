# Monitor and receive notifications on table changes

[![license](https://img.shields.io/github/license/mashape/apistatus.svg)](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/LICENSE.txt) [![license](https://img.shields.io/badge/release-6.1.0-brightgreen.svg)](#) [![date](https://img.shields.io/badge/date-May%2026%2C%202017-orange.svg)](#) [![NuGet Badge](https://buildstats.info/nuget/SqlTableDependency)](https://www.nuget.org/packages/SqlTableDependency/) [![SQL Server](https://img.shields.io/badge/SQL%20Server-%3E%3D2012-RED.svg)](#) [![.NET](https://img.shields.io/badge/.NET-%3E%3D%204.5.1-ff69b4.svg)](#)

**SqlTableDependency** is a high-level C# component used to audit, monitor and receive notifications on SQL Server's record table changes.

For any record table change, as insert, update or delete operation, a notification **containing values for the record changed** is received from SqlTableDependency. This notification contains the update values from the database table.

<img src="https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/Workflow-min.png" />

This tracking change system has the advantage to avoid a database select to retrieve updated table record, because the updated table values record is delivered to you by notification.

## Track record table changes
If we want **get alert about record table changes** without paying attention to the underlying SQL Server infrastructure then SqlTableDependency's record table change notifications will do that for us. Using notifications, an application can **detect table record changes** saving us from having to continuously re-query the database to get new values: for any record change, SqlTableDependency's event handler will get a notification containing modified table record values as well as the insert, update, delete operation type executed on our table.

Assuming we are interested to receive record changes for the following database table:

<img src="https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/rsz_table.jpg" />

Start installing SqlTableDependency using:

[![IMAGE ALT TEXT HERE](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/NuGetSqlTableDependency.png)](https://www.nuget.org/packages/SqlTableDependency/)

We define a C# model object mapping table columns we are interested to be populated with the values from any INSERT, DELETE or UPDATE operation. We do not need to define all table columns: just the ones we are interested to:

```C#
public class Customer
{
    public int Id { get; set; }
    public string Name { get; set; }
    public string Surname { get; set; }
}
```
Properties can have a different name from table column. We see later as to establish a mapping.

Create the SqlTableDependency object passing the connection string and table name (table name in sonly necessary when C# model name is different from table name). Then create an event handler for SqlTableDependency's Changed event:

```C#
using System;
using TableDependency.SqlClient;
using TableDependency.Enums;
using TableDependency.Events;

class Program
{
   var _con= "data source=.; initial catalog=MyDB; integrated security=True";
   
   static void Main()
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

   static void Changed(object sender, RecordChangedEventArgs<Customer> e)
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

[![IMAGE ALT TEXT HERE](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/Receive_notifications_from_Sql_Server_database.gif)](https://www.youtube.com/watch?v=sHJVusS5Qz0)

### Monitor table changes use cases and examples
Here are some examples of application getting notification on record table change. After downoad the example, plese remember to update SqlTableDependency nuget package:

* [Monitor table change with WPF and WCF](https://github.com/christiandelbianco/Monitor-table-change-with-WPF-WCF-sqltabledependency): This example show how to keep up to date a grid containing some stocks data. That grid has been automatically updated whenever a record change using database notifications. This notification contains new values for the modified table record.
* [Monitor table change with MVC, SignalR and jQuery](https://github.com/christiandelbianco/monitor-table-change-with-mvc-signalR-jquery-sqltabledependency): This example show how to keep up to date a table containing some stocks data. That table has been automatically updated whenever a record change using database notifications. This notification contains new values for the modified table record.
* [Monitor table change with MVC, SignalR and Knockout JS](https://github.com/christiandelbianco/monitor-table-change-with-mvc-signalR-knockoutjs-sqltabledependency): This example show how to refresh client web browsers used to book flight tickets. Those terminals have to be update as soon as the availability change and the Web application must take the initiative of sending this information to clients instead of waiting for the client to request it. 

This section reports some use case examples. Some of these examples, use OracleTableDependency; this is not ena more supported. However, the example is still valid for SqlTableDepdendcy:

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
<blockquote>
The Start(int timeOut = 120, int watchDogTimeOut = 180) method runs the listener to receive record change notifications.
The watchDogTimeOut parameter specify the amount of time in seconds for the watch dog system.

Calling Stop() method, notifications are not any more delivered and all SqlTableDependency's database objects are deleted. 

It is a good practice - when possible - wrap SqlTableDependency within a using statement or alternatively in a try catch block: when the application will stop, this is enough to remove the SqlTableDependency infrastructure (Trigger, Service Broker service, the queue, Contract, Messages type and Stored Procedure) automatically.

However, when the application exits abruptly – that is not calling the Stop() method or not implementing the using statement - we need a way for cleaning up the SqlTableDependency infrastructure. The Start() method, has watchDogTimeOut optional parameter used to remove all the database objects. Its default value is 180 seconds: after this amount of time, if there are no listeners waiting for notifications, the SqlTableDependency infrastructure will be removed. This time seems long enough. Or not?

There is one very common scenario that results in much more time: debugging. When you develop applications, you often spend several minutes inside the debugger before you move on. So please be careful when you debug an application that the value assigned to watchDogTimeOut parameter is long enough, otherwise you will incur in a destruction of database objects in the middle of you debug activity.
</blockquote>

#### Under The Hood
SqlTableDependency's record change audit, provides the low-level implementation to receive database notifications creating SQL Server trigger, queue and service broker that immediately notify us when any record table changes happens.

Assuming we want monitor the Customer table contents, we create a SqlTableDependency object specifying the Customer table and the following database objects will be generated:
* Message types
* Contract
* Queue
* Service Broker
* Trigger on table to be monitored
* Stored procedure to clean up the created objects in case the application exits abruptly (that is, when the application terminate without disposing the SqlTableDependency object)

![alt text][DatabaseObjects]

[DatabaseObjects]: https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/DbObjects-min.png "Database Object created for send notifications"

#### Requirements
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

It is also possible skip the permissions test done from SqlTableDependency, setting executeUserPermissionCheck constructor parameter to false. In this case, If the user does not have sufficient permissions, a SQL server exception will be thrown.

#### Donate
SqlTableDependency is a personal open source project. Started in 2015, I have put hundreds of hours adding new features, enhancing and fixes, with the goal to make them a usefull and  user-friendly component. I need your help to achieve this.

[![Donate](https://pledgie.com/campaigns/30269.png)](https://pledgie.com/campaigns/30269)

#### Contributors
Please, feel free to help and contribute with this project adding your comments, issues or bugs found as well as proposing fix and enhancements. [See contributors](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Contributors).

#### Useful link
* http://msftsbprodsamples.codeplex.com/
* https://stackoverflow.com/questions/41169144/sqltabledependency-onchange-event-not-fired
* https://stackoverflow.com/questions/11383145/sql-server-2008-service-broker-tutorial-cannot-receive-the-message-exception
