using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class TwoIntancesTest : Base.SqlTableDependencyBaseTest
    {
        private class TwoIntancesModel
        {
            public long Id { get; set; }
            public string Name { get; set; }
        }

        private const string TableName1 = "TwoIntancesModel1";
        private const string TableName2 = "TwoIntancesModel2";
        private static readonly Dictionary<string, IList<TwoIntancesModel>> CheckValues1 = new Dictionary<string, IList<TwoIntancesModel>>();
        private static readonly Dictionary<string, IList<TwoIntancesModel>> CheckValues2 = new Dictionary<string, IList<TwoIntancesModel>>();

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName1}', 'U') IS NOT NULL DROP TABLE [{TableName1}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName1}]([Id] [int] NULL, [Name] [NVARCHAR](50) NULL)";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName2}', 'U') IS NOT NULL DROP TABLE [{TableName2}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName2}]([Id] [int] NULL, [Name] [NVARCHAR](50) NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }

            CheckValues1.Add(ChangeType.Insert.ToString(), new List<TwoIntancesModel>());
            CheckValues1.Add(ChangeType.Update.ToString(), new List<TwoIntancesModel>());
            CheckValues1.Add(ChangeType.Delete.ToString(), new List<TwoIntancesModel>());

            CheckValues2.Add(ChangeType.Insert.ToString(), new List<TwoIntancesModel>());
            CheckValues2.Add(ChangeType.Update.ToString(), new List<TwoIntancesModel>());
            CheckValues2.Add(ChangeType.Delete.ToString(), new List<TwoIntancesModel>());
        }

        [TestInitialize]
        public void TestInitialize()
        {
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName1}', 'U') IS NOT NULL DROP TABLE [{TableName1}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName2}', 'U') IS NOT NULL DROP TABLE [{TableName2}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void Test()
        {
            SqlTableDependency<TwoIntancesModel> tableDependency1 = null;
            SqlTableDependency<TwoIntancesModel> tableDependency2 = null;
            string naming1 = null;
            string naming2 = null;

            try
            {
                tableDependency1 = new SqlTableDependency<TwoIntancesModel>(ConnectionStringForTestUser, tableName: TableName1);
                tableDependency1.OnChanged += TableDependency_Changed1;
                naming1 = tableDependency1.DataBaseObjectsNamingConvention;
                tableDependency2 = new SqlTableDependency<TwoIntancesModel>(ConnectionStringForTestUser, tableName: TableName2);
                tableDependency2.OnChanged += TableDependency_Changed2;
                naming2 = tableDependency2.DataBaseObjectsNamingConvention;

                tableDependency1.Start();
                tableDependency2.Start();

                var t1 = new Task(ModifyTableContent1);
                var t2 = new Task(ModifyTableContent2);

                t1.Start();
                t2.Start();

                Thread.Sleep(1000 * 30 * 1);
            }
            finally
            {
                tableDependency1?.Dispose();
                tableDependency2?.Dispose();
            }

            Assert.IsTrue(CheckValues1[ChangeType.Insert.ToString()].All(m => m.Id == 1 && m.Name == "Luciano Bruschi"));
            Assert.IsTrue(CheckValues1[ChangeType.Insert.ToString()].Count == 50);
            Assert.IsTrue(CheckValues1[ChangeType.Update.ToString()].All(m => m.Id == 2 && m.Name == "Ceccarelli Velia"));
            Assert.IsTrue(CheckValues1[ChangeType.Update.ToString()].Count == 50);
            Assert.IsTrue(CheckValues1[ChangeType.Delete.ToString()].All(m => m.Id == 2 && m.Name == "Ceccarelli Velia"));
            Assert.IsTrue(CheckValues1[ChangeType.Delete.ToString()].Count == 50);

            Assert.IsTrue(CheckValues2[ChangeType.Insert.ToString()].All(m => m.Id == 1 && m.Name == "Christian Del Bianco"));
            Assert.IsTrue(CheckValues2[ChangeType.Insert.ToString()].Count == 50);
            Assert.IsTrue(CheckValues2[ChangeType.Update.ToString()].All(m => m.Id == 2 && m.Name == "Dina Bruschi"));
            Assert.IsTrue(CheckValues2[ChangeType.Update.ToString()].Count == 50);
            Assert.IsTrue(CheckValues2[ChangeType.Delete.ToString()].All(m => m.Id == 2 && m.Name == "Dina Bruschi"));
            Assert.IsTrue(CheckValues2[ChangeType.Delete.ToString()].Count == 50);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming1));
            Assert.IsTrue(base.CountConversationEndpoints(naming1) == 0);
            Assert.IsTrue(base.AreAllDbObjectDisposed(naming2));
            Assert.IsTrue(base.CountConversationEndpoints(naming2) == 0);
        }

        private static void TableDependency_Changed1(object sender, RecordChangedEventArgs<TwoIntancesModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues1[ChangeType.Insert.ToString()].Add(new TwoIntancesModel { Name = e.Entity.Name, Id = e.Entity.Id });
                    break;
                case ChangeType.Update:
                    CheckValues1[ChangeType.Update.ToString()].Add(new TwoIntancesModel { Name = e.Entity.Name, Id = e.Entity.Id });
                    break;
                case ChangeType.Delete:
                    CheckValues1[ChangeType.Delete.ToString()].Add(new TwoIntancesModel { Name = e.Entity.Name, Id = e.Entity.Id });
                    break;
            }
        }

        private static void TableDependency_Changed2(object sender, RecordChangedEventArgs<TwoIntancesModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues2[ChangeType.Insert.ToString()].Add(new TwoIntancesModel { Name = e.Entity.Name, Id = e.Entity.Id });
                    break;
                case ChangeType.Update:
                    CheckValues2[ChangeType.Update.ToString()].Add(new TwoIntancesModel { Name = e.Entity.Name, Id = e.Entity.Id });
                    break;
                case ChangeType.Delete:
                    CheckValues2[ChangeType.Delete.ToString()].Add(new TwoIntancesModel { Name = e.Entity.Name, Id = e.Entity.Id });
                    break;
            }
        }

        private static void ModifyTableContent1()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                for (int i = 0; i < 50; i++)
                {
                    using (var sqlCommand = sqlConnection.CreateCommand())
                    {
                        sqlCommand.CommandText = $"INSERT INTO [{TableName1}] ([Id], [Name]) VALUES (1, 'Luciano Bruschi')";
                        sqlCommand.ExecuteNonQuery();
                    }

                    using (var sqlCommand = sqlConnection.CreateCommand())
                    {
                        sqlCommand.CommandText = $"UPDATE [{TableName1}] SET [Id] = 2, [Name] = 'Ceccarelli Velia'";
                        sqlCommand.ExecuteNonQuery();
                    }

                    using (var sqlCommand = sqlConnection.CreateCommand())
                    {
                        sqlCommand.CommandText = $"DELETE FROM [{TableName1}]";
                        sqlCommand.ExecuteNonQuery();
                    }
                }
            }
        }

        private static void ModifyTableContent2()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    for (int i = 0; i < 50; i++)
                    {
                        sqlCommand.CommandText = $"INSERT INTO [{TableName2}] ([Id], [Name]) VALUES (1, 'Christian Del Bianco')";
                        sqlCommand.ExecuteNonQuery();

                        sqlCommand.CommandText = $"UPDATE [{TableName2}] SET [Id] = 2, [Name] = 'Dina Bruschi'";
                        sqlCommand.ExecuteNonQuery();

                        sqlCommand.CommandText = $"DELETE FROM [{TableName2}]";
                        sqlCommand.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}