using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.SqlServer;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    internal class Issue27Model
    {
        public string Id { get; set; }
        public string Message { get; set; }
    }

    [TestClass]
    public class Issue27
    {
        private const string TableName = "Issue27Model";
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServerConnectionString"].ConnectionString;

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
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


        public TestContext TestContext { get; set; }


        [TestCategory("SqlServer")]
        [TestMethod]
        public void Issue27Tesst()
        {
            try
            {
                string objectNaming;

                using (var tableDependency = new SqlTableDependency<Issue27Model>(ConnectionString, TableName))
                {
                    tableDependency.OnChanged += TableDependency_Changed;
                    tableDependency.Start();
                    objectNaming = tableDependency.DataBaseObjectsNamingConvention;

                    Thread.Sleep(5000);                    
                }

                Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, objectNaming));
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