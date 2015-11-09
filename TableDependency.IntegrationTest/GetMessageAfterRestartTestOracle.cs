using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.DataAccess.Client;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.IntegrationTest.Helpers.SqlServer;
using TableDependency.OracleClient;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    public class GetMessageAfterRestartTestSqlOracleModel
    {
        public string Name { get; set; }
    }

    [TestClass]
    public class GetMessageAfterRestartTestOracleTest
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        public static readonly string TableName = "ANNNoi".ToUpper();
        public static string NamingToUse = "AMESSAGEAFTERRESTART";

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            OracleHelper.DropTable(ConnectionString, TableName);
        }

        [TestInitialize()]
        public void TestInitialize()
        {
            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {TableName} (NAME VARCHAR2(50))";
                    command.ExecuteNonQuery();
                }
            }
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            OracleHelper.DropTable(ConnectionString, TableName);
        }

        /// <summary>
        /// Tests this instance.
        /// </summary>
        [TestMethod]
        public void Test()
        {
            using (var tableDependency = new OracleTableDependency<GetMessageAfterRestartTestSqlOracleModel>(
                ConnectionString,
                TableName,
                mapper: null,
                updateOf: (List<string>)null,
                automaticDatabaseObjectsTeardown: false,
                namingConventionForDatabaseObjects: NamingToUse))
            {
                tableDependency.OnChanged += (object sender, RecordChangedEventArgs<GetMessageAfterRestartTestSqlOracleModel> e) => { };
                tableDependency.Start();
            }

            Thread.Sleep(5 * 60 * 1000);
            Assert.IsFalse(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, NamingToUse));
            ModifyTableContent();


            var domaininfo = new AppDomainSetup { ApplicationBase = Environment.CurrentDirectory };
            var adevidence = AppDomain.CurrentDomain.Evidence;
            var domain = AppDomain.CreateDomain("AppDomainGMessageAfterRestart", adevidence, domaininfo);
            var otherDomainObject = (AppDomainGMessageAfterRestart)domain.CreateInstanceAndUnwrap(typeof(AppDomainGMessageAfterRestart).Assembly.FullName, typeof(AppDomainGMessageAfterRestart).FullName);
            var nameUsed = otherDomainObject.RunTableDependency(ConnectionString, TableName, NamingToUse);
            Thread.Sleep(5 * 60 * 1000);
            var checkValues = otherDomainObject.GetResult();
            otherDomainObject.DisposeTableDependency();
            AppDomain.Unload(domain);

            var results = checkValues.Split(',');
            Assert.AreEqual("Valentina", results[0]);
            Assert.AreEqual("Christian", results[1]);
            Assert.AreEqual("Christian", results[2]);
            Assert.IsTrue(OracleHelper.AreAllDbObjectDisposed(ConnectionString, nameUsed));
        }

        private static void ModifyTableContent()
        {
            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var sqlCommand = connection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO {TableName} (NAME) VALUES ('Valentina')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"UPDATE {TableName} SET NAME = 'Christian'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"DELETE FROM {TableName}";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);
                }
            }
        }
    }

    public class AppDomainGMessageAfterRestart : MarshalByRefObject
    {
        public OracleTableDependency<GetMessageAfterRestartTestSqlOracleModel> TableDependency;
        private readonly List<string> _checkValues = new List<string>();

        public string RunTableDependency(string connectionString, string tableName, string namingToUse)
        {
            this.TableDependency = new OracleTableDependency<GetMessageAfterRestartTestSqlOracleModel>(connectionString,
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

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<GetMessageAfterRestartTestSqlOracleModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues.Add(e.Entity.Name);
                    break;
                case ChangeType.Update:
                    _checkValues.Add(e.Entity.Name);
                    break;
                case ChangeType.Delete:
                    _checkValues.Add(e.Entity.Name);
                    break;
                case ChangeType.None:
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}