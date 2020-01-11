using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class Issue177Test : Base.SqlTableDependencyBaseTest
    {
        private class Issue177Model
        {
            public int Id { get; set; }
            public string Message { get; set; }
        }

        private static readonly Dictionary<string, Issue177Model> CheckValues = new Dictionary<string, Issue177Model>();
        private static readonly string TableName = typeof(Issue177Model).Name;

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
                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [NCHAR](16) NOT NULL, [Message] [NVARCHAR](100) NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }

            CheckValues.Clear();

            CheckValues.Add(ChangeType.Insert.ToString(), new Issue177Model());
            CheckValues.Add(ChangeType.Update.ToString(), new Issue177Model());
            CheckValues.Add(ChangeType.Delete.ToString(), new Issue177Model());
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
                string objectNaming;

                using (var tableDependency = new SqlTableDependency<Issue177Model>(ConnectionStringForTestUser, tableName: TableName))
                {
                    tableDependency.OnChanged += TableDependency_Changed;
                    tableDependency.OnError += TableDependency_OnError;
                    objectNaming = tableDependency.DataBaseObjectsNamingConvention;
                    tableDependency.Start();

                    Thread.Sleep(5000);
                    var t = new Task(ModifyTableContent);
                    t.Start();

                    Thread.Sleep(1000 * 15 * 1);
                }

                Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Id, 1);
                Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Message, "Cat");

                Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Id, 1234);
                Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Message, "Cat");

                Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Id, 1234);
                Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Message, "Cat");

                Assert.IsTrue(base.AreAllDbObjectDisposed(objectNaming));
                Assert.IsTrue(base.CountConversationEndpoints(objectNaming) == 0);
            }
            catch (Exception exception)
            {
                Assert.Fail(exception.Message);
            }
        }

        private static void TableDependency_OnError(object sender, ErrorEventArgs e)
        {
            Assert.Fail(e.Error.Message);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Issue177Model> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Id = e.Entity.Id;
                    CheckValues[ChangeType.Insert.ToString()].Message = e.Entity.Message;
                    break;

                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Id = e.Entity.Id;
                    CheckValues[ChangeType.Update.ToString()].Message = e.Entity.Message;
                    break;

                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Id = e.Entity.Id;
                    CheckValues[ChangeType.Delete.ToString()].Message = e.Entity.Message;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Message]) VALUES ('1', 'Cat')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Id] = '1234' WHERE [Message] = 'Cat'";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]  WHERE [Message] = 'Cat'";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}