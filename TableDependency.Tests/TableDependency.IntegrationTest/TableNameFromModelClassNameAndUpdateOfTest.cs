using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Base;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    public class TableNameFromModelClassNameAndUpdateOfTestSqlServerModel
    {
        public long Id { get; set; }

        public string Name { get; set; }

        [Column(ColumnName)]
        public string FamilyName { get; set; }

        private const string ColumnName = "SURNAME";

        public static string GetColumnName => ColumnName;
    }

    [TestClass]
    public class TableNameFromModelClassNameAndUpdateOfTest : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(TableNameFromModelClassNameAndUpdateOfTestSqlServerModel).Name.ToUpper();
        private static readonly Dictionary<string, Tuple<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel, TableNameFromModelClassNameAndUpdateOfTestSqlServerModel>> CheckValues = new Dictionary<string, Tuple<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel, TableNameFromModelClassNameAndUpdateOfTestSqlServerModel>>();
        private static int _counter;

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

                    sqlCommand.CommandText =
                        $"CREATE TABLE [{TableName}]( " +
                        $"[Id] [int] IDENTITY(1, 1) NOT NULL, " +
                        $"[Name] [NVARCHAR](50) NULL, " +
                        $"[Surname] [NVARCHAR](MAX) NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
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
        public void Test()
        {
            SqlTableDependency<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel> tableDependency = null;
            string naming;

            try
            {
                UpdateOfModel<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel> updateOf = new UpdateOfModel<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel>();
                updateOf.Add(model => model.FamilyName);

                tableDependency = new SqlTableDependency<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel>(ConnectionStringForTestUser, updateOf: updateOf);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 5 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 2);

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.FamilyName, CheckValues[ChangeType.Insert.ToString()].Item1.FamilyName);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.FamilyName, CheckValues[ChangeType.Delete.ToString()].Item1.FamilyName);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming)== 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.Id = e.Entity.Id;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Insert.ToString()].Item2.FamilyName = e.Entity.FamilyName;
                    break;

                case ChangeType.Delete:
                   
                    CheckValues[ChangeType.Delete.ToString()].Item2.Id = e.Entity.Id;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Delete.ToString()].Item2.FamilyName = e.Entity.FamilyName;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel, TableNameFromModelClassNameAndUpdateOfTestSqlServerModel>(new TableNameFromModelClassNameAndUpdateOfTestSqlServerModel { Id = 23, Name = "Pizza Mergherita", FamilyName = "Pizza Mergherita" }, new TableNameFromModelClassNameAndUpdateOfTestSqlServerModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel, TableNameFromModelClassNameAndUpdateOfTestSqlServerModel>(new TableNameFromModelClassNameAndUpdateOfTestSqlServerModel { Id = 23, Name = "Pizza Funghi", FamilyName = "Pizza Mergherita" }, new TableNameFromModelClassNameAndUpdateOfTestSqlServerModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<TableNameFromModelClassNameAndUpdateOfTestSqlServerModel, TableNameFromModelClassNameAndUpdateOfTestSqlServerModel>(new TableNameFromModelClassNameAndUpdateOfTestSqlServerModel { Id = 23, Name = "Pizza Funghi", FamilyName = "Pizza Mergherita" }, new TableNameFromModelClassNameAndUpdateOfTestSqlServerModel()));

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name], [SURNAME]) VALUES ('{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', '{CheckValues[ChangeType.Insert.ToString()].Item1.FamilyName}')";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = '{CheckValues[ChangeType.Update.ToString()].Item1.Name}'";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}