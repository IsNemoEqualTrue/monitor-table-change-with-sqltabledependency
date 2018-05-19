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
    public class Issue66Test : SqlTableDependencyBaseTest
    {
        private class Issue66Model
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public string City { get; set; }
        }

        private static readonly string TableName = typeof(Issue66Model).Name;
        private static readonly Dictionary<string, List<Issue66Model>> CheckValues = new Dictionary<string, List<Issue66Model>>();
        private static readonly Dictionary<string, List<Issue66Model>> CheckValuesOld = new Dictionary<string, List<Issue66Model>>();

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
                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}] ([Id] [INT] NULL, [Name] [NVARCHAR(50)] NULL, [City] [NVARCHAR(50)] NULL)";
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
            CheckValuesOld.Clear();

            CheckValues.Add(ChangeType.Insert.ToString(), new List<Issue66Model>());
            CheckValues.Add(ChangeType.Update.ToString(), new List<Issue66Model>());
            CheckValues.Add(ChangeType.Delete.ToString(), new List<Issue66Model>());

            CheckValuesOld.Add(ChangeType.Insert.ToString(), new List<Issue66Model>());
            CheckValuesOld.Add(ChangeType.Update.ToString(), new List<Issue66Model>());
            CheckValuesOld.Add(ChangeType.Delete.ToString(), new List<Issue66Model>());
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
            SqlTableDependency<Issue66Model> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<Issue66Model>(ConnectionStringForTestUser, includeOldValues: true);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()][0].Id, 1);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()][0].Name, "CHRISTIAN");
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()][0].City, "LAVENA PONTE TRESA");
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()][1].Id, 2);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()][1].Name, "VALENTINA");
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()][1].City, "LAVENA PONTE TRESA");

            Assert.AreEqual(CheckValuesOld[ChangeType.Insert.ToString()].Count, 0);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()][0].City, "BAAR");
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()][1].City, "BAAR");

            Assert.AreEqual(CheckValuesOld[ChangeType.Update.ToString()][0].City, "LAVENA PONTE TRESA");
            Assert.AreEqual(CheckValuesOld[ChangeType.Update.ToString()][1].City, "LAVENA PONTE TRESA");

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()][0].Id, 1);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()][0].Name, "christian");
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()][0].City, "BAAR");
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()][1].Id, 2);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()][1].Name, "valentina");
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()][1].City, "BAAR");

            Assert.AreEqual(CheckValuesOld[ChangeType.Delete.ToString()], 0);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<Issue66Model> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Add(e.Entity);
                    CheckValuesOld[ChangeType.Insert.ToString()].Add(e.EntityOldValues);
                    break;

                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Add(e.Entity);
                    CheckValuesOld[ChangeType.Update.ToString()].Add(e.EntityOldValues);
                    break;

                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Add(e.Entity);
                    CheckValuesOld[ChangeType.Delete.ToString()].Add(e.EntityOldValues);
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
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name], [City]) VALUES(1, 'CHRISTIAN', 'LAVENA PONTE TRESA')";                    
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name], [City]) VALUES(2, 'VALENTINA', 'LAVENA PONTE TRESA')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [City] = 'BAAR', [Name] = LOWER([Name])";
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