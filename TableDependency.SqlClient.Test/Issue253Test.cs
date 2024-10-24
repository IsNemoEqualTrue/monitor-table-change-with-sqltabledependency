using System;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class Issue253Test : Base.SqlTableDependencyBaseTest
    {
        private class Issue253Model
        {
            public string Id { get; set; }
        }

        private static readonly string TableName = nameof(Issue253Model);

        private Exception _unobservedTaskException;

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('[{TableName}]', 'U') IS NOT NULL DROP TABLE [dbo].[{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [int] NULL)";
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
        public void Test()
        {
            try
            {
                string naming;

                using (var tableDependency = new SqlTableDependency<Issue253Model>(ConnectionStringForTestUser, tableName: TableName))
                {
                    tableDependency.OnChanged += (o, args) => { };
                    tableDependency.Start();
                    naming = tableDependency.DataBaseObjectsNamingConvention;

                    Thread.Sleep(5000);

                    TaskScheduler.UnobservedTaskException += TaskSchedulerOnUnobservedTaskException;
                    try
                    {
                        tableDependency.Stop();

                        Thread.Sleep(5000);

                        GC.Collect();
                        GC.WaitForPendingFinalizers();
                    }
                    finally
                    {
                        TaskScheduler.UnobservedTaskException -= TaskSchedulerOnUnobservedTaskException;
                    }
                }

                Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
                Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
            }
            catch (Exception exception)
            {
                Assert.Fail(exception.Message);
            }

            Assert.IsNull(_unobservedTaskException);
        }

        private void TaskSchedulerOnUnobservedTaskException(object sender, UnobservedTaskExceptionEventArgs e)
        {
            _unobservedTaskException = e.Exception;
        }
    }
}