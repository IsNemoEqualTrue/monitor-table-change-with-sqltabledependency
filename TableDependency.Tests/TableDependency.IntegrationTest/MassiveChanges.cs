using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Base;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    public class MassiveChangesModel
    {
        public long Id { get; set; }
        public string Name { get; set; }
    }

    [TestClass]
    public class MassiveChanges : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(MassiveChangesModel).Name;
        private static Dictionary<string, IList<MassiveChangesModel>> CheckValues = new Dictionary<string, IList<MassiveChangesModel>>();

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

            CheckValues.Add(ChangeType.Insert.ToString(), new List<MassiveChangesModel>());
            CheckValues.Add(ChangeType.Update.ToString(), new List<MassiveChangesModel>());
            CheckValues.Add(ChangeType.Delete.ToString(), new List<MassiveChangesModel>());
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
            SqlTableDependency<MassiveChangesModel> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<MassiveChangesModel>(ConnectionStringForTestUser);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 30 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.IsTrue(CheckValues[ChangeType.Insert.ToString()].All(m => m.Id == 1 && m.Name == "Luciano Bruschi"));
            Assert.IsTrue(CheckValues[ChangeType.Insert.ToString()].Count == 100);
            Assert.IsTrue(CheckValues[ChangeType.Update.ToString()].All(m => m.Id == 2 && m.Name == "Ceccarelli Velia"));
            Assert.IsTrue(CheckValues[ChangeType.Update.ToString()].Count == 100);
            Assert.IsTrue(CheckValues[ChangeType.Delete.ToString()].All(m => m.Id == 2 && m.Name == "Ceccarelli Velia"));
            Assert.IsTrue(CheckValues[ChangeType.Delete.ToString()].Count == 100);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<MassiveChangesModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Add(new MassiveChangesModel { Name = e.Entity.Name, Id = e.Entity.Id });
                    break;
                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Add(new MassiveChangesModel { Name = e.Entity.Name, Id = e.Entity.Id });
                    break;
                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Add(new MassiveChangesModel { Name = e.Entity.Name, Id = e.Entity.Id });
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
                    for (int i = 0; i < 100; i++)
                    {
                        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name]) VALUES (1, 'Luciano Bruschi')";
                        sqlCommand.ExecuteNonQuery();

                        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Id] = 2, [Name] = 'Ceccarelli Velia'";
                        sqlCommand.ExecuteNonQuery();

                        sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                        sqlCommand.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}