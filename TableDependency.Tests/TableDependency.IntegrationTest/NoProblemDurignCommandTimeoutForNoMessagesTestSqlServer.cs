using System;
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
    public class NoProblemDurignCommandTimeoutForNoMessagesModelSql
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
        public DateTime Born { get; set; }
        public int Quantity { get; set; }
    }

    [TestClass]
    public class NoProblemDurignCommandTimeoutForNoMessagesSqlServer
    {

        private static string _dbObjectsNaming;
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServerConnectionString"].ConnectionString;
        private static string TableName = "Check_Model";

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

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id][int] IDENTITY(1, 1) NOT NULL, [First Name] [NVARCHAR](50) NOT NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestCategory("SqlServer")]
        [TestMethod()]
        public void Test()
        {
            var domaininfo = new AppDomainSetup();
            domaininfo.ApplicationBase = Environment.CurrentDirectory;
            var adevidence = AppDomain.CurrentDomain.Evidence;
            var domain = AppDomain.CreateDomain("TableDependencyDomaing", adevidence, domaininfo);
            var otherDomainObject = (RunsInAnotherAppDomainNoMessage) domain.CreateInstanceAndUnwrap(typeof (RunsInAnotherAppDomainNoMessage).Assembly.FullName, typeof (RunsInAnotherAppDomainNoMessage).FullName);
            _dbObjectsNaming = otherDomainObject.RunTableDependency(ConnectionString, TableName);
            Thread.Sleep(4*60*1000);
            var status = otherDomainObject.GetTableDependencyStatus();
            AppDomain.Unload(domain);
            Thread.Sleep(3*60*1000);

            Assert.IsTrue(status != TableDependencyStatus.StoppedDueToError && status != TableDependencyStatus.StoppedDueToCancellation);
            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, _dbObjectsNaming));
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

        public class RunsInAnotherAppDomainNoMessage : MarshalByRefObject
        {
            private SqlTableDependency<NoProblemDurignCommandTimeoutForNoMessagesModelSql> _tableDependency = null;

            public TableDependencyStatus GetTableDependencyStatus()
            {
                return this._tableDependency.Status;
            }

            public string RunTableDependency(string connectionString, string tableName)
            {
                var mapper = new ModelToTableMapper<NoProblemDurignCommandTimeoutForNoMessagesModelSql>();
                mapper.AddMapping(c => c.Name, "First Name");

                this._tableDependency = new SqlTableDependency<NoProblemDurignCommandTimeoutForNoMessagesModelSql>(connectionString, tableName, mapper);
                this._tableDependency.OnChanged += TableDependency_Changed;
                this._tableDependency.Start(60, 120);
                return this._tableDependency.DataBaseObjectsNamingConvention;
            }

            private static void TableDependency_Changed(object sender, RecordChangedEventArgs<NoProblemDurignCommandTimeoutForNoMessagesModelSql> e)
            {
            }
        }
    }
}