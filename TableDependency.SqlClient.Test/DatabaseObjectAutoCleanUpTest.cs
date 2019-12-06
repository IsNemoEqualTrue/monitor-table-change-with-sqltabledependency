using System;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Base;
using TableDependency.SqlClient.Test.Inheritance;

namespace TableDependency.SqlClient.Test
{
    public class DatabaseObjectCleanUpTestSqlServerModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
        public DateTime Born { get; set; }
        public int Quantity { get; set; }
    }

    [TestClass]
    public class DatabaseObjectCleanUpTest : Base.SqlTableDependencyBaseTest
    {
        private static string _dbObjectsNaming;
        private const string TableName = "DatabaseObjectCleanUpTestTable";

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText =
                        $"CREATE TABLE [{TableName}]( " +
                        "[Id][int] IDENTITY(1, 1) NOT NULL, " +
                        "[First Name] [nvarchar](50) NOT NULL, " +
                        "[Second Name] [nvarchar](50) NOT NULL, " +
                        "[Born] [datetime] NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TestInsertAfterStop()
        {
            var mapper = new ModelToTableMapper<DatabaseObjectCleanUpTestSqlServerModel>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second Name");

            var tableDependency = new SqlTableDependency<DatabaseObjectCleanUpTestSqlServerModel>(
                ConnectionStringForTestUser,
                includeOldValues: true,
                tableName: TableName,
                mapper: mapper);

            tableDependency.OnChanged += (sender, e) => { };
            tableDependency.Start();
            var dbObjectsNaming = tableDependency.DataBaseObjectsNamingConvention;

            var t = new Task(BigModifyTableContent);
            t.Start();
            Thread.Sleep(1000 * 15 * 1);

            tableDependency.Stop();

            SmalModifyTableContent();

            Thread.Sleep(1 * 15 * 1000);
            Assert.IsTrue(base.AreAllDbObjectDisposed(dbObjectsNaming));
            Assert.IsTrue(base.CountConversationEndpoints(dbObjectsNaming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TestCleanUpAfter2InsertsTest()
        {
            var mapper = new ModelToTableMapper<DatabaseObjectCleanUpTestSqlServerModel>();
            mapper.AddMapping(c => c.Name, "First Name").AddMapping(c => c.Surname, "Second Name");

            var tableDependency = new SqlTableDependencyTest<DatabaseObjectCleanUpTestSqlServerModel>(
                ConnectionStringForTestUser,
                includeOldValues: true,
                tableName: TableName,
                mapper: mapper);

            tableDependency.OnChanged += (sender, e) => { }; 
            tableDependency.Start();
            var dbObjectsNaming = tableDependency.DataBaseObjectsNamingConvention;

            Thread.Sleep(500);

            tableDependency.Stop();

            var t = new Task(ModifyTableContent);
            t.Start();

            Thread.Sleep(1000 * 15 * 1);

            Assert.IsTrue(base.AreAllDbObjectDisposed(dbObjectsNaming));
            Assert.IsTrue(base.CountConversationEndpoints(dbObjectsNaming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TestCleanUpAfterHugeInserts()
        {
            var mapper = new ModelToTableMapper<DatabaseObjectCleanUpTestSqlServerModel>();
            mapper.AddMapping(c => c.Name, "First Name").AddMapping(c => c.Surname, "Second Name");

            var tableDependency = new SqlTableDependencyTest<DatabaseObjectCleanUpTestSqlServerModel>(
                ConnectionStringForTestUser,
                includeOldValues: true,
                tableName: TableName,
                mapper: mapper);

            tableDependency.OnChanged += (o, args) => { };
            tableDependency.Start();
            var dbObjectsNaming = tableDependency.DataBaseObjectsNamingConvention;

            Thread.Sleep(5000);

            tableDependency.Stop();

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    for (var i = 0; i < 10000; i++)
                    {
                        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{i}', '{i}')";
                        sqlCommand.ExecuteNonQuery();
                    }
                }
                sqlConnection.Close();
            }

            Thread.Sleep(1000 * 15 * 1);

            Assert.IsTrue(base.AreAllDbObjectDisposed(dbObjectsNaming));
            Assert.IsTrue(base.CountConversationEndpoints(dbObjectsNaming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TestStopWhileStillInserting()
        {
            SqlTableDependency<DatabaseObjectCleanUpTestSqlServerModel> tableDependency = new SqlTableDependency<DatabaseObjectCleanUpTestSqlServerModel>(
                ConnectionStringForTestUser,
                tableName: TableName);

            string objectNaming = tableDependency.DataBaseObjectsNamingConvention;

            tableDependency.OnChanged += (sender, e) => { };
            objectNaming = tableDependency.DataBaseObjectsNamingConvention;
            tableDependency.Start();

            Thread.Sleep(1000);

            // Run async tasks insering 1000 rows in table every 250 milliseconds
            var task1 = Task.Factory.StartNew(() => ModifyTableContent());
            var task2 = Task.Factory.StartNew(() => ModifyTableContent());
            var task3 = Task.Factory.StartNew(() => ModifyTableContent());

            Thread.Sleep(5000);

            tableDependency.Stop();
            Thread.Sleep(5000);

            Assert.IsTrue(base.AreAllDbObjectDisposed(objectNaming));
            Assert.IsTrue(base.CountConversationEndpoints(objectNaming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TestCollapsingTheAppDomain()
        {
            var domaininfo = new AppDomainSetup { ApplicationBase = Environment.CurrentDirectory };
            var adevidence = AppDomain.CurrentDomain.Evidence;
            var domain = AppDomain.CreateDomain("RunsInAnotherAppDomain_Check_DatabaseObjectCleanUp", adevidence, domaininfo);
            var otherDomainObject = (RunsInAnotherAppDomainCheckDatabaseObjectCleanUp)domain.CreateInstanceAndUnwrap(typeof(RunsInAnotherAppDomainCheckDatabaseObjectCleanUp).Assembly.FullName, typeof(RunsInAnotherAppDomainCheckDatabaseObjectCleanUp).FullName);
            _dbObjectsNaming = otherDomainObject.RunTableDependency(ConnectionStringForTestUser, tableName: TableName);
            Thread.Sleep(1000);

            // Run async tasks insering 1000 rows in table every 250 milliseconds
            var task1 = Task.Factory.StartNew(() => ModifyTableContent());
            var task2 = Task.Factory.StartNew(() => ModifyTableContent());
            var task3 = Task.Factory.StartNew(() => ModifyTableContent());

            // Wait 5 seconds and then collapse the app domain where sqltabledependency is running
            Thread.Sleep(5000);
            AppDomain.Unload(domain);

            // After 3 minutes, even if the background thread is still inserting data in table, db objects must be removed
            Thread.Sleep(3 * 60 * 1000);
            Assert.IsTrue(base.AreAllDbObjectDisposed(_dbObjectsNaming));
            Assert.IsTrue(base.CountConversationEndpoints(_dbObjectsNaming) == 0);

            // Wait a minute in order to let the task complete and not interfeer with other tests!
            Thread.Sleep(1 * 60 * 1000);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TestThrowExceptionInCreateSqlServerDatabaseObjects()
        {
            SqlTableDependencyTest<DatabaseObjectCleanUpTestSqlServerModel> tableDependency = null;
            string objectNaming = string.Empty;

            try
            {
                tableDependency = new SqlTableDependencyTest<DatabaseObjectCleanUpTestSqlServerModel>(
                    ConnectionStringForTestUser,
                    tableName: TableName,
                    throwExceptionCreateSqlServerDatabaseObjects: true);

                tableDependency.OnChanged += (sender, e) => { };
                objectNaming = tableDependency.DataBaseObjectsNamingConvention;
                tableDependency.Start();
            }
            catch
            {

            }

            Assert.IsTrue(base.AreAllDbObjectDisposed(objectNaming));
            Assert.IsTrue(base.CountConversationEndpoints(objectNaming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TestThrowExceptionInWaitForNotificationsPoint3()
        {
            SqlTableDependencyTest<DatabaseObjectCleanUpTestSqlServerModel> tableDependency = null;
            string objectNaming = string.Empty;

            try
            {
                tableDependency = new SqlTableDependencyTest<DatabaseObjectCleanUpTestSqlServerModel>(
                    ConnectionStringForTestUser,
                    tableName: TableName,
                    throwExceptionInWaitForNotificationsPoint3: true);

                tableDependency.OnChanged += (sender, e) => { };
                objectNaming = tableDependency.DataBaseObjectsNamingConvention;
                tableDependency.Start();
            }
            catch
            {

            }

            Thread.Sleep(1000 * 60 * 4);
            Assert.IsTrue(base.AreAllDbObjectDisposed(objectNaming));
            Assert.IsTrue(base.CountConversationEndpoints(objectNaming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TestThrowExceptionInWaitForNotificationsPoint2()
        {
            SqlTableDependencyTest<DatabaseObjectCleanUpTestSqlServerModel> tableDependency = null;
            string objectNaming = string.Empty;

            try
            {
                tableDependency = new SqlTableDependencyTest<DatabaseObjectCleanUpTestSqlServerModel>(
                    ConnectionStringForTestUser,
                    tableName: TableName,
                    throwExceptionInWaitForNotificationsPoint2: true);

                tableDependency.OnChanged += (sender, e) => { };
                objectNaming = tableDependency.DataBaseObjectsNamingConvention;
                tableDependency.Start();
            }
            catch
            {

            }

            Thread.Sleep(1000 * 60 * 4);
            Assert.IsTrue(base.AreAllDbObjectDisposed(objectNaming));
            Assert.IsTrue(base.CountConversationEndpoints(objectNaming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TestThrowExceptionInWaitForNotificationsPoint1()
        {
            SqlTableDependencyTest<DatabaseObjectCleanUpTestSqlServerModel> tableDependency = null;
            string objectNaming = string.Empty;

            try
            {
                tableDependency = new SqlTableDependencyTest<DatabaseObjectCleanUpTestSqlServerModel>(
                    ConnectionStringForTestUser,
                    tableName: TableName,
                    throwExceptionInWaitForNotificationsPoint1: true);

                tableDependency.OnChanged += (sender, e) => { };
                objectNaming = tableDependency.DataBaseObjectsNamingConvention;
                tableDependency.Start();
            }
            catch
            {

            }

            Thread.Sleep(1000 * 60 * 4);
            Assert.IsTrue(base.AreAllDbObjectDisposed(objectNaming));
            Assert.IsTrue(base.CountConversationEndpoints(objectNaming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TestStartWitError()
        {
            SqlTableDependencyTest<DatabaseObjectCleanUpTestSqlServerModel> tableDependency = null;
            string objectNaming = string.Empty;

            try
            {
                tableDependency = new SqlTableDependencyTest<DatabaseObjectCleanUpTestSqlServerModel>(
                    ConnectionStringForTestUser,
                    tableName: TableName,
                    throwExceptionBeforeWaitForNotifications: true);

                tableDependency.OnChanged += (sender, e) => { };
                objectNaming = tableDependency.DataBaseObjectsNamingConvention;
                tableDependency.Start();
            }
            catch
            {

            }

            Thread.Sleep(1000 * 60 * 4);
            Assert.IsTrue(base.AreAllDbObjectDisposed(objectNaming));
            Assert.IsTrue(base.CountConversationEndpoints(objectNaming) == 0);
        }

        private static void ModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{Guid.NewGuid().ToString()}', 'mah')";
                        sqlCommand.ExecuteNonQuery();

                        Thread.Sleep(250);
                    }
                }
            }
        }

        private static void BigModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    for (var i = 0; i < 100000; i++)
                    {
                        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{i}', '{i}')";
                        sqlCommand.ExecuteNonQuery();
                    }
                }
            }
        }

        private static void SmalModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('allora', 'mah')";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }

    public class RunsInAnotherAppDomainCheckDatabaseObjectCleanUp : MarshalByRefObject
    {
        public string RunTableDependency(string connectionString, string tableName)
        {
            var mapper = new ModelToTableMapper<DatabaseObjectCleanUpTestSqlServerModel>();
            mapper.AddMapping(c => c.Name, "First Name").AddMapping(c => c.Surname, "Second Name");

            var tableDependency = new SqlTableDependency<DatabaseObjectCleanUpTestSqlServerModel>(connectionString, tableName: tableName, mapper: mapper);
            tableDependency.OnChanged += (sender, e) => { };
            tableDependency.Start(60, 120);
            return tableDependency.DataBaseObjectsNamingConvention;
        }
    }
}