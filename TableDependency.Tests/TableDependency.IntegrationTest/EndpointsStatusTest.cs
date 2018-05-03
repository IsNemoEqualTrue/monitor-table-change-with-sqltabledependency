using System;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.IntegrationTest.Base;
using TableDependency.SqlClient;
using TableDependency.SqlClient.Enumerations;

namespace TableDependency.IntegrationTest
{
    public class EndpointsStatusModel
    {
        public long Id { get; set; }
    }

    [TestClass]
    public class EndpointsStatusTest : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(EndpointsStatusModel).Name;

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForSa))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"CREATE TABLE {TableName} ([Id] BIGINT NULL)";
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
            using (var sqlConnection = new SqlConnection(ConnectionStringForSa))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID ('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void Test()
        {
            bool startReceivingMessages = false;

            var tableDependency = new SqlTableDependency<EndpointsStatusModel>(ConnectionStringForTestUser);
            tableDependency.OnChanged += (o, args) => { startReceivingMessages = true; };
            tableDependency.Start();
            var naming = tableDependency.DataBaseObjectsNamingConvention;

            Assert.IsTrue(this.IsSenderEndpointInStatus(naming, ConversationEndpointState.SO));
            Assert.IsTrue(this.IsReceiverEndpointInStatus(naming, null));

            var t = new Task(InsertRecord);
            t.Start();
            while (startReceivingMessages == false) Thread.Sleep(1000 * 1 * 1);

            Assert.IsTrue(this.IsSenderEndpointInStatus(naming, ConversationEndpointState.CO));
            Assert.IsTrue(this.IsReceiverEndpointInStatus(naming, ConversationEndpointState.CO));

            tableDependency.Stop();

            Thread.Sleep(1000 * 30 * 1);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private static void InsertRecord()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id]) VALUES ({DateTime.Now.Ticks})"; sqlCommand.ExecuteNonQuery();
                }
            }
        }

        private bool IsSenderEndpointInStatus(string objectNaming, ConversationEndpointState? status)
        {
            var state = this.RetrieveEndpointStatus($"{objectNaming}_Receiver");

            return state == status;
        }

        private bool IsReceiverEndpointInStatus(string objectNaming, ConversationEndpointState? status)
        {
            var state = this.RetrieveEndpointStatus($"{objectNaming}_Sender");

            return state == status;
        }

        private ConversationEndpointState? RetrieveEndpointStatus(string farService)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForSa))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"select [state] from sys.conversation_endpoints WITH (NOLOCK) where [far_service] = '{farService}';";
                    var state = (string)sqlCommand.ExecuteScalar();

                    return string.IsNullOrWhiteSpace(state) ? (ConversationEndpointState?)null : (ConversationEndpointState)Enum.Parse(typeof(ConversationEndpointState), state);
                }
            }
        }
    }
}