using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient.BaseTests;

namespace TableDependency.SqlClient.IntegrationTests
{
    public class StatusTestSqlServerModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
        public DateTime Born { get; set; }
        public int Quantity { get; set; }
    }

    [TestClass]
    public class StatusTest : SqlTableDependencyBaseTest
    {
        private SqlTableDependency<StatusTestSqlServerModel> _tableDependency;
        private static readonly string TableName = typeof(StatusTestSqlServerModel).Name;
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
                var mapper = new ModelToTableMapper<StatusTestSqlServerModel>();
                mapper.AddMapping(c => c.Name, "FIRST name");
                mapper.AddMapping(c => c.Surname, "Second Name");
                this._tableDependency = new SqlTableDependency<StatusTestSqlServerModel>(ConnectionStringForTestUser, tableName: TableName, mapper: mapper);
                this._tableDependency.OnChanged += this.TableDependency_Changed;
                this._tableDependency.OnStatusChanged += this.TableDependency_OnStatusChanged;
                this._tableDependency.OnError += this.TableDependency_OnError;
                var dataBaseObjectsNamingConvention = _tableDependency.DataBaseObjectsNamingConvention;

                this._tableDependency.Start();

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);

                this._tableDependency.Stop();
                Thread.Sleep(1000 * 15 * 1);

                Assert.IsTrue(Statuses[TableDependencyStatus.Starting]);
                Assert.IsTrue(Statuses[TableDependencyStatus.Started]);
                Assert.IsTrue(Statuses[TableDependencyStatus.WaitingForNotification]);
                Assert.IsTrue(Statuses[TableDependencyStatus.StopDueToCancellation]);
                Assert.IsFalse(Statuses[TableDependencyStatus.StopDueToError]);

                Assert.IsTrue(base.AreAllDbObjectDisposed(dataBaseObjectsNamingConvention));
                Assert.IsTrue(base.CountConversationEndpoints(dataBaseObjectsNamingConvention) == 0);
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

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<StatusTestSqlServerModel> e)
        {

        }

        private static void ModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('Ismano', 'Del Bianco')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = 'Dina', [Second Name] = 'Bruschi'";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}