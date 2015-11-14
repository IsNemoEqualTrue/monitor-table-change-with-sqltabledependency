using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.SqlServer;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    public class GetMessageAfterRestartTestSqlServerModel
    {
        public string Name { get; set; }
    }

    [TestClass]
    public class GetMessageAfterRestartTestSqlServer
    {
        public static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServerConnectionString"].ConnectionString;
        public static readonly string TableName = "ANoDispose";
        public static string NamingToUse = "AAA_GetMessageAfterRestartTestSqlServer";

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Name] [NVARCHAR](50) NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize()]
        public void TestInitialize()
        {
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
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
        public void Test()
        {
            using (var tableDependency = new SqlTableDependency<GetMessageAfterRestartTestSqlServerModel>(
                ConnectionString,
                TableName,
                mapper: null,
                updateOf: (List<string>)null,
                automaticDatabaseObjectsTeardown: false,
                namingConventionForDatabaseObjects: NamingToUse))
            {
                tableDependency.OnChanged += (object sender, RecordChangedEventArgs<GetMessageAfterRestartTestSqlServerModel> e) => { };
                tableDependency.Start(60, 120);
            }

            Thread.Sleep(2 * 60 * 1000);
            Assert.IsFalse(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, NamingToUse));
            ModifyTableContent();


            var domaininfo = new AppDomainSetup { ApplicationBase = Environment.CurrentDirectory };
            var adevidence = AppDomain.CurrentDomain.Evidence;
            var domain = AppDomain.CreateDomain("AppDomGetMessageAfterRestart", adevidence, domaininfo);
            var otherDomainObject = (AppDomGetMessageAfterRestart)domain.CreateInstanceAndUnwrap(typeof(AppDomGetMessageAfterRestart).Assembly.FullName, typeof(AppDomGetMessageAfterRestart).FullName);
            var nameUsed = otherDomainObject.RunTableDependency(ConnectionString, TableName, NamingToUse);
            Thread.Sleep(1 * 60 * 1000);
            var checkValues = otherDomainObject.GetResult();
            otherDomainObject.DisposeTableDependency();
            AppDomain.Unload(domain);

            var results = checkValues.Split(',');
            Assert.AreEqual("Valentina", results[0]);
            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, nameUsed));
        }

        private static void ModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name]) VALUES ('Valentina')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);
                }
            }
        }
    }

    public class AppDomGetMessageAfterRestart : MarshalByRefObject
    {
        public SqlTableDependency<GetMessageAfterRestartTestSqlServerModel> TableDependency;
        private readonly List<string> _checkValues = new List<string>();

        public string RunTableDependency(string connectionString, string tableName, string namingToUse)
        {
            this.TableDependency = new SqlTableDependency<GetMessageAfterRestartTestSqlServerModel>(connectionString,
                tableName,
                mapper: null,
                updateOf: (List<string>)null,
                automaticDatabaseObjectsTeardown: true,
                namingConventionForDatabaseObjects: namingToUse);

            this.TableDependency.OnChanged += TableDependency_Changed;
            this.TableDependency.Start(60, 120);
            return this.TableDependency.DataBaseObjectsNamingConvention;
        }

        public string GetResult()
        {
            return string.Join(",", _checkValues);
        }

        public void DisposeTableDependency()
        {
            this.TableDependency.Stop();
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<GetMessageAfterRestartTestSqlServerModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues.Add(e.Entity.Name);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}