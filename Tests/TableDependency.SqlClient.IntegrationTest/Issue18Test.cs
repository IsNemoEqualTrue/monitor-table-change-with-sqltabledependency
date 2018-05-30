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
    [TestClass]
    public class Issue18Test : SqlTableDependencyBaseTest
    {
        private class Issue18Model
        {
            public int Id { get; set; }
            public decimal Price { get; set; }
        }

        private static readonly string TableName = typeof(Issue18Model).Name;
        private static readonly Dictionary<string, Issue18Model> CheckValues = new Dictionary<string, Issue18Model>();

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
                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [int] NULL, [Price] [float])";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize]
        public void TestInitialize()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }

            CheckValues.Clear();

            CheckValues.Add(ChangeType.Insert.ToString(), new Issue18Model());
            CheckValues.Add(ChangeType.Update.ToString(), new Issue18Model());
            CheckValues.Add(ChangeType.Delete.ToString(), new Issue18Model());
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
            var tableDependency = new SqlTableDependency<Issue18Model>(ConnectionStringForTestUser);
            string objectNaming;

            try
            {
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                objectNaming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency.Dispose();
            }


            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Id, 1);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Price, 123.0001002000000100M);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Id, 1);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Price, 1234.0002003000000000M);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Id, 1);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Price, 1234.0002003000000000M);

            Assert.IsTrue(base.AreAllDbObjectDisposed(objectNaming));
            Assert.IsTrue(base.CountConversationEndpoints(objectNaming) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Issue18Model> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Id = e.Entity.Id;
                    CheckValues[ChangeType.Insert.ToString()].Price = e.Entity.Price;
                    break;

                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Id = e.Entity.Id;
                    CheckValues[ChangeType.Update.ToString()].Price = e.Entity.Price;
                    break;

                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Id = e.Entity.Id;
                    CheckValues[ChangeType.Delete.ToString()].Price = e.Entity.Price;
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
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Price]) VALUES (1, 123.0001002)";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Price] = 1234.0002003 WHERE [Id] = 1";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]  WHERE [Id] = 1";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}