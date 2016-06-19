using System;
using System.Linq;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
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
    public class StatusTestSqlServer
    {        
        private SqlTableDependency<StatusTestSqlServerModel> _tableDependency = null;
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServerConnectionString"].ConnectionString;
        private const string TableName = "StatusCheckTest";
        private static IDictionary<TableDependencyStatus, bool> statuses = new Dictionary<TableDependencyStatus, bool>();

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

        [TestInitialize()]
        public void TestInitialize()
        {
            statuses.Add(TableDependencyStatus.Starting, false);
            statuses.Add(TableDependencyStatus.Started, false);
            statuses.Add(TableDependencyStatus.WaitingForNotification, false);
            statuses.Add(TableDependencyStatus.MessageReadyToBeNotified, false);
            statuses.Add(TableDependencyStatus.MessageSent, false);
            statuses.Add(TableDependencyStatus.StoppedDueToCancellation, false);
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
        public void StatusTest()
        {
            try
            {
                var mapper = new ModelToTableMapper<StatusTestSqlServerModel>();
                mapper.AddMapping(c => c.Name, "FIRST name");
                mapper.AddMapping(c => c.Surname, "Second Name");
                this._tableDependency = new SqlTableDependency<StatusTestSqlServerModel>(ConnectionString, TableName, mapper);
                this._tableDependency.OnChanged += this.TableDependency_Changed;
                this._tableDependency.OnStatusChanged += this.TableDependency_OnStatusChanged;

                this._tableDependency.Start();

                Thread.Sleep(1 * 60 * 1000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000);

                this._tableDependency.Stop();

                foreach (var status in statuses)
                {
                    Assert.IsTrue(statuses[status.Key] == true);
                }                
            }
            finally
            {
                this._tableDependency?.Dispose();
            }
        }

        private void TableDependency_OnStatusChanged(object sender, StatusChangedEventArgs e)
        {
            statuses[e.Status] = true;
            Assert.IsTrue(e.Status == this._tableDependency.Status);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<StatusTestSqlServerModel> e)
        {
            
        }

        private static void ModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('Ismano', 'Del Bianco')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = 'Dina', [Second Name] = 'Bruschi'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);
                }
            }
        }
    }
}