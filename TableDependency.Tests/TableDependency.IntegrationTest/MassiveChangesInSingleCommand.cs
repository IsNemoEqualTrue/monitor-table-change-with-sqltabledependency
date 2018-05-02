using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Base;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    public class MassiveChangesInSingleCommandModel
    {
        public long Id { get; set; }
        public string Name { get; set; }
    }

    [TestClass]
    public class MassiveChangesInSingleCommand : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(MassiveChangesInSingleCommandModel).Name;
        private static Dictionary<string, IList<MassiveChangesInSingleCommandModel>> CheckValues = new Dictionary<string, IList<MassiveChangesInSingleCommandModel>>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [int] NULL, [Name] [NVARCHAR](50) NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }

            CheckValues.Add(ChangeType.Insert.ToString(), new List<MassiveChangesInSingleCommandModel>());
            CheckValues.Add(ChangeType.Update.ToString(), new List<MassiveChangesInSingleCommandModel>());
            CheckValues.Add(ChangeType.Delete.ToString(), new List<MassiveChangesInSingleCommandModel>());
        }

        [TestInitialize()]
        public void TestInitialize()
        {
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

        [TestCategory("SqlServer")]
        [TestMethod]
        public void EventForAllColumnsTest()
        {
            SqlTableDependency<MassiveChangesInSingleCommandModel> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<MassiveChangesInSingleCommandModel>(ConnectionStringForTestUser);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                var t1 = new Task(ModifyTableContent1);
                var t2 = new Task(ModifyTableContent2);

                t1.Start();
                t2.Start();

                Thread.Sleep(1000 * 30 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.IsTrue(CheckValues[ChangeType.Insert.ToString()].All(m => (m.Id == 1 || m.Id == 3) && (m.Name == "Luciano Bruschi" || m.Name == "Dina Bruschi")));
            Assert.IsTrue(CheckValues[ChangeType.Insert.ToString()].Count == 20);
            Assert.IsTrue(CheckValues[ChangeType.Update.ToString()].All(m => (m.Id == 2 || m.Id == 4) && (m.Name == "Ceccarelli Velia" || m.Name == "Ismano Del Bianco")));
            Assert.IsTrue(CheckValues[ChangeType.Update.ToString()].Count == 20);
            Assert.IsTrue(CheckValues[ChangeType.Delete.ToString()].All(m => (m.Id == 2 || m.Id == 4) && (m.Name == "Ceccarelli Velia" || m.Name == "Ismano Del Bianco")));
            Assert.IsTrue(CheckValues[ChangeType.Delete.ToString()].Count == 20);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<MassiveChangesInSingleCommandModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Add(new MassiveChangesInSingleCommandModel { Name = e.Entity.Name, Id = e.Entity.Id });
                    break;
                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Add(new MassiveChangesInSingleCommandModel { Name = e.Entity.Name, Id = e.Entity.Id });
                    break;
                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Add(new MassiveChangesInSingleCommandModel { Name = e.Entity.Name, Id = e.Entity.Id });
                    break;
            }
        }

        private static void ModifyTableContent1()
        {
            var commandText = new StringBuilder("BEGIN ");
            for (int i = 0; i < 10; i++)
            {
                commandText.Append($"INSERT INTO [{TableName}] ([Id], [Name]) VALUES (1, 'Luciano Bruschi');");
                commandText.Append($"UPDATE [{TableName}] SET [Id] = 2, [Name] = 'Ceccarelli Velia' WHERE [Id] = 1;");
                commandText.Append($"DELETE FROM [{TableName}] WHERE [Id] = 2;");
            }
            commandText.Append(" END");

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = commandText.ToString();
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(100);
                }
            }
        }

        private static void ModifyTableContent2()
        {
            var commandText = new StringBuilder();
            for (int i = 0; i < 10; i++)
            {
                commandText.Append($"INSERT INTO [{TableName}] ([Id], [Name]) VALUES (3, 'Dina Bruschi');");
                commandText.Append($"UPDATE [{TableName}] SET [Id] = 4, [Name] = 'Ismano Del Bianco' WHERE [Id] = 3;");
                commandText.Append($"DELETE FROM [{TableName}] WHERE [Id] = 4");
            }

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = commandText.ToString();
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(100);
                }
            }
        }
    }
}