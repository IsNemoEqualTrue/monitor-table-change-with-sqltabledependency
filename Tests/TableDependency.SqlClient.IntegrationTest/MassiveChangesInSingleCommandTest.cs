using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient.BaseTests;

namespace TableDependency.SqlClient.IntegrationTests
{
    [TestClass]
    public class MassiveChangesInSingleCommandTest : SqlTableDependencyBaseTest
    {
        private class MassiveChangesInSingleCommandModel
        {
            public long Id { get; set; }
            public string Name { get; set; }
        }

        private static readonly string TableName = typeof(MassiveChangesInSingleCommandModel).Name;
        private static Dictionary<string, IList<MassiveChangesInSingleCommandModel>> _checkValues = new Dictionary<string, IList<MassiveChangesInSingleCommandModel>>();
        private static Dictionary<string, IList<MassiveChangesInSingleCommandModel>> _checkValuesOld = new Dictionary<string, IList<MassiveChangesInSingleCommandModel>>();

        [ClassInitialize]
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

            _checkValues.Clear();
            _checkValuesOld.Clear();
            
            _checkValues.Add(ChangeType.Insert.ToString(), new List<MassiveChangesInSingleCommandModel>());
            _checkValues.Add(ChangeType.Update.ToString(), new List<MassiveChangesInSingleCommandModel>());
            _checkValues.Add(ChangeType.Delete.ToString(), new List<MassiveChangesInSingleCommandModel>());

            _checkValuesOld.Add(ChangeType.Insert.ToString(), new List<MassiveChangesInSingleCommandModel>());
            _checkValuesOld.Add(ChangeType.Update.ToString(), new List<MassiveChangesInSingleCommandModel>());
            _checkValuesOld.Add(ChangeType.Delete.ToString(), new List<MassiveChangesInSingleCommandModel>());
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
            SqlTableDependency<MassiveChangesInSingleCommandModel> tableDependency = null;
            string naming;

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

            Assert.IsTrue(_checkValues[ChangeType.Insert.ToString()].All(m => (m.Id == 1 || m.Id == 3) && (m.Name == "Luciano Bruschi" || m.Name == "Dina Bruschi")));
            Assert.IsTrue(_checkValues[ChangeType.Insert.ToString()].Count == 20);
            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.IsTrue(_checkValues[ChangeType.Update.ToString()].All(m => (m.Id == 2 || m.Id == 4) && (m.Name == "Ceccarelli Velia" || m.Name == "Ismano Del Bianco")));
            Assert.IsTrue(_checkValues[ChangeType.Update.ToString()].Count == 20);
            Assert.IsNull(_checkValuesOld[ChangeType.Update.ToString()]);

            Assert.IsTrue(_checkValues[ChangeType.Delete.ToString()].All(m => (m.Id == 2 || m.Id == 4) && (m.Name == "Ceccarelli Velia" || m.Name == "Ismano Del Bianco")));
            Assert.IsTrue(_checkValues[ChangeType.Delete.ToString()].Count == 20);
            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TestWithOldValues()
        {
            SqlTableDependency<MassiveChangesInSingleCommandModel> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<MassiveChangesInSingleCommandModel>(ConnectionStringForTestUser, includeOldValues: true);
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

            Assert.IsTrue(_checkValues[ChangeType.Insert.ToString()].All(m => (m.Id == 1 || m.Id == 3) && (m.Name == "Luciano Bruschi" || m.Name == "Dina Bruschi")));
            Assert.IsTrue(_checkValues[ChangeType.Insert.ToString()].Count == 20);
            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.IsTrue(_checkValues[ChangeType.Update.ToString()].All(m => (m.Id == 2 || m.Id == 4) && (m.Name == "Ceccarelli Velia" || m.Name == "Ismano Del Bianco")));
            Assert.IsTrue(_checkValues[ChangeType.Update.ToString()].Count == 20);
            Assert.IsTrue(_checkValuesOld[ChangeType.Update.ToString()].All(m => (m.Id == 1 || m.Id == 3) && (m.Name == "Luciano Bruschi" || m.Name == "Dina Bruschi")));
            Assert.IsTrue(_checkValuesOld[ChangeType.Update.ToString()].Count == 20);

            Assert.IsTrue(_checkValues[ChangeType.Delete.ToString()].All(m => (m.Id == 2 || m.Id == 4) && (m.Name == "Ceccarelli Velia" || m.Name == "Ismano Del Bianco")));
            Assert.IsTrue(_checkValues[ChangeType.Delete.ToString()].Count == 20);
            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);


            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<MassiveChangesInSingleCommandModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Add(new MassiveChangesInSingleCommandModel { Name = e.Entity.Name, Id = e.Entity.Id });

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()].Add(new MassiveChangesInSingleCommandModel { Name = e.EntityOldValues.Name, Id = e.EntityOldValues.Id });
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()] = null;
                    }

                    break;

                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Add(new MassiveChangesInSingleCommandModel { Name = e.Entity.Name, Id = e.Entity.Id });

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Update.ToString()].Add(new MassiveChangesInSingleCommandModel { Name = e.EntityOldValues.Name, Id = e.EntityOldValues.Id });
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Update.ToString()] = null;
                    }

                    break;

                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Add(new MassiveChangesInSingleCommandModel { Name = e.Entity.Name, Id = e.Entity.Id });
                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Delete.ToString()].Add(new MassiveChangesInSingleCommandModel { Name = e.EntityOldValues.Name, Id = e.EntityOldValues.Id });
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Delete.ToString()] = null;
                    }

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
                }
            }
        }
    }
}