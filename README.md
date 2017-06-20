# Monitor and receive notifications on table changes

SqlTableDependency is a high-level C# component to used to audit, monitor and receive notifications on SQL Server's record table changes.

For any record table change, insert update or delete, a notification **containing values for the record** **inserted**, **changed** or **deleted** is received from SqlTableDependency. This notification contains the update values int the database table.

![alt text][Workflow]

[Workflow]: https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/Workflow-min.png "Notifications"

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
CREATE TABLE [dbo].[Customers](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[First Name] [nvarchar](50) NOT NULL,
	[Second Name] [nvarchar](50) NOT NULL,
	[Birthday] [datetime] NOT NULL,
	[DepartmentId] [int] NOT NULL)
```

Install SqlTableDependency using:

[![IMAGE ALT TEXT HERE](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/blob/master/NuGetSqlTableDependency.png)](https://www.nuget.org/packages/SqlTableDependency/)

Write your model defining interested table columns:
```C#
public class Customer
{
    public int Id { get; set; }
    public string Name { get; set; }
    public string Surname { get; set; }
}
```
The model can avoid to define all table columns if you are not interested in some value. Also, model's properties name can be different from database table columns name.

Create the SqlTableDependency object passing the connection string and table name (table name is necessary because of model name is different from table name). Then create an event handler for SqlTableDependency's Changed event:

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
       // The mappar is use to link model properties with a name that do not match table columns name
       var mapper = new ModelToTableMapper<Customer>();
       mapper.AddMapping(c => c.Surname, "Second Name");
       mapper.AddMapping(c => c.Name, "First Name");

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
       if (e.ChangeType != ChangeType.None)
       {
           var changedEntity = e.Entity;
           Console.WriteLine("DML operation: " + e.ChangeType);
           Console.WriteLine("ID: " + changedEntity.Id);
           Console.WriteLine("Name: " + changedEntity.Name);
           Console.WriteLine("Surame: " + changedEntity.Surname);
       }
   }
}
```

Done! Now you are ready to receive notifications. Open SQL Server management studio and insert, update or delete some record in the Customers table:

[![IMAGE ALT TEXT HERE](http://img.youtube.com/vi/sHJVusS5Qz0/0.jpg)](https://www.youtube.com/watch?v=sHJVusS5Qz0)

## Use cases and more examples
Here are some examples of application getting notification on record table change. After downoad the example, plese remember to update SqlTableDependency nuget package:

* [Monitor table change with WPF and WCF](https://github.com/christiandelbianco/Monitor-table-change-with-WPF-WCF-sqltabledependency): This example show how to keep up to date a grid containing some stocks data. That grid has been automatically updated whenever a record change using database notifications. This notification contains new values for the modified table record.
* [Monitor table change with MVC, SignalR and jQuery](https://github.com/christiandelbianco/monitor-table-change-with-mvc-signalR-jquery-sqltabledependency): This example show how to keep up to date a table containing some stocks data. That table has been automatically updated whenever a record change using database notifications. This notification contains new values for the modified table record.
* [Monitor table change with MVC, SignalR and Knockout JS](https://github.com/christiandelbianco/monitor-table-change-with-mvc-signalR-knockoutjs-sqltabledependency): This example show how to refresh client web browsers used to book flight tickets. Those terminals have to be update as soon as the availability change and the Web application must take the initiative of sending this information to clients instead of waiting for the client to request it. 

This section reports some use case examples. Some of these examples, use OracleTableDependency; this is not ena more supported. However, the example is still valid for SqlTableDepdendcy:

* [Code First Data Annotations to map model with database table.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Code-First-Data-Annotations-to-map-model-with-database-table)
* [Custom map between model property and table column using ModelToTableMapper<T>.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Custom-map-between-model-property-and-table-column-using-ModelToTableMapper-T-)
* [Explicit database table name.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Explicit-database-table-name)
* [Filter notification by operation type.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Filter-notification-by-operation-type)
* [Get Errors.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Get-errors)
* [Logging.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Logging)
* [Model and properties with same name of table and columns.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Model-and-properties-with-same-name-of-table-and-columns)
* [Specify for which properties we want receive notification using UpdateOfModel<T> mapper.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Specify-for-which-properties-we-want-receive-notification-using-UpdateOfModel-T--mapper)
* [Get Status.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Status-change)
* [Apply filter based on WHERE condition.](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Use-case:-Where-filter)

## Donate
TableDependency, SqlTableDependency and OracleTableDependency are personal open source projects. Started in 2015, I have put hundreds of hours adding new features, enhancing and fixes, with the goal to make them a usefull and a user friendly component. I need your help to achieve this.

[![Donate](https://pledgie.com/campaigns/30269.png)](https://pledgie.com/campaigns/30269)

## Contributors
Please, feel free to help and contribute with this project adding your comments, issues or bugs found as well as proposing fix and enhancements.

[See contributors](https://github.com/christiandelbianco/monitor-table-change-with-sqltabledependency/wiki/Contributors)

## Contacts
Christian Del Bianco<br/>
Mail: christian.delbianco@gmail.com<br/>
Skype: christian.delbianco<br/>
