using System;
using System.Data.SqlClient;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Base;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    internal class Issue27Model
    {
        public string Id { get; set; }
        public string Message { get; set; }
    }

    [TestClass]
    public class Issue27 : SqlTableDependencyBaseTest
    {
        private const string TableName = "Issue27Model";
        
        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('[{TableName}]', 'U') IS NOT NULL DROP TABLE [dbo].[{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [int] NULL, [Message] [VARCHAR](100) NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [ClassCleanup()]
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


        public TestContext TestContext { get; set; }


        [TestCategory("SqlServer")]
        [TestMethod]
        public void Issue27Tesst()
        {
            try
            {
                string objectNaming;

                using (var tableDependency = new SqlTableDependency<Issue27Model>(ConnectionStringForTestUser, TableName))
                {
                    tableDependency.OnChanged += TableDependency_Changed;
                    tableDependency.Start();
                    objectNaming = tableDependency.DataBaseObjectsNamingConvention;

                    Thread.Sleep(5000);                    
                }

                Assert.IsTrue(base.AreAllDbObjectDisposed(objectNaming));
                Assert.IsTrue(base.CountConversationEndpoints(objectNaming) == 0);
            }
            catch (Exception exception)
            {
                TestContext.WriteLine(exception.Message);
                Assert.Fail();
            }
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Issue27Model> e)
        {

        }
    }
}