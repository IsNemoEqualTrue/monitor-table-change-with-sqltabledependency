using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Base;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;
using TableDependency.SqlClient.Test.Base;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class StatusLostConnectionTest : Base.SqlTableDependencyBaseTest
    {
        public class StatusLostConnectionTestModel
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public string Surname { get; set; }
        }

        private SqlTableDependency<StatusLostConnectionTestModel> _tableDependency;
        private static readonly string TableName = typeof(StatusLostConnectionTestModel).Name;
        private static readonly IDictionary<TableDependencyStatus, bool> Statuses = new Dictionary<TableDependencyStatus, bool>();

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
                        "[First Name] [NVARCHAR](50) NOT NULL, " +
                        "[Second Name] [NVARCHAR](50) NOT NULL, " +
                        "[Born] [DATETIME] NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize]
        public void TestInitialize()
        {
            Statuses.Add(TableDependencyStatus.Starting, false);
            Statuses.Add(TableDependencyStatus.Started, false);
            Statuses.Add(TableDependencyStatus.WaitingForNotification, false);
            Statuses.Add(TableDependencyStatus.StopDueToCancellation, false);
            Statuses.Add(TableDependencyStatus.StopDueToError, false);
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
        public void Test()
        {
            try
            {
                var mapper = new ModelToTableMapper<StatusLostConnectionTestModel>();
                mapper.AddMapping(c => c.Name, "FIRST name");
                mapper.AddMapping(c => c.Surname, "Second Name");
                this._tableDependency = new SqlTableDependency<StatusLostConnectionTestModel>(ConnectionStringForTestUser, tableName: TableName, mapper: mapper);
                this._tableDependency.OnChanged += this.TableDependency_Changed;
                this._tableDependency.OnStatusChanged += this.TableDependency_OnStatusChanged;
                this._tableDependency.OnError += this.TableDependency_OnError;
                var dataBaseObjectsNamingConvention = _tableDependency.DataBaseObjectsNamingConvention;

                this._tableDependency.Start();

                var taskModifyTableContent = new Task(ModifyTableContent);
                taskModifyTableContent.Start();
                Thread.Sleep(1000 * 15 * 1);

                var taskKillSqlTableDependencyDbConnection = new Task(KillSqlTableDependencyDbConnection);
                taskKillSqlTableDependencyDbConnection.Start();
                Thread.Sleep(1000 * 15 * 1);

                Assert.IsTrue(Statuses[TableDependencyStatus.Starting]);
                Assert.IsTrue(Statuses[TableDependencyStatus.Started]);
                Assert.IsTrue(Statuses[TableDependencyStatus.WaitingForNotification]);
                Assert.IsFalse(Statuses[TableDependencyStatus.StopDueToCancellation]);

                Assert.IsTrue(Statuses[TableDependencyStatus.StopDueToError]);           
                Assert.IsTrue(_tableDependency.Status == TableDependencyStatus.StopDueToError);
            }
            finally
            {
                this._tableDependency?.Dispose();
            }
        }

        private void TableDependency_OnError(object sender, ErrorEventArgs e)
        {
            throw e.Error;
        }

        private void TableDependency_OnStatusChanged(object sender, StatusChangedEventArgs e)
        {
            Statuses[e.Status] = true;
            Assert.IsTrue(e.Status == this._tableDependency.Status);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<StatusLostConnectionTestModel> e)
        {

        }

        private static void ModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(SqlTableDependencyBaseTest.ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('Ismano', 'Del Bianco')";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        private static void KillSqlTableDependencyDbConnection()
        {
            using (var sqlConnection = new SqlConnection(SqlTableDependencyBaseTest.ConnectionStringForSa))
            {
                var sqlConnectionStringBuilder = new SqlConnectionStringBuilder(SqlTableDependencyBaseTest.ConnectionStringForTestUser);
                var initialCatalog = sqlConnectionStringBuilder.InitialCatalog;
                var userId = sqlConnectionStringBuilder.UserID;

                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DECLARE @kill varchar(8000); SET @kill = ''; SELECT @kill = @kill + 'kill ' + CONVERT(varchar(5), spid) + ';' FROM master..sysprocesses WHERE dbid = db_id('{initialCatalog}') and loginame = '{userId}'; EXEC(@kill);";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}