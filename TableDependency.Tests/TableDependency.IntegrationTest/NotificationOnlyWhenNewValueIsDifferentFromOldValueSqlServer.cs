using System;
using System.Collections.Generic;
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
    public class NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
    }

    [TestClass]
    public class NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServer : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel).Name;
        private static int _counter1;
        private static int _counter2;
        private static int _counter3;
        private static readonly List<Tuple<NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel, NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel>> CheckValues1 = new List<Tuple<NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel, NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel>>();
        private static readonly List<Tuple<NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel, NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel>> CheckValues2 = new List<Tuple<NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel, NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel>>();

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

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id][int] IDENTITY(1, 1) NOT NULL, [First Name] [NVARCHAR](50) NULL, [Second Name] [NVARCHAR](50) NULL, [NickName] [NVARCHAR](50) NULL)";
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

        #region UpdateOneInterestedColumn

        [TestCategory("SqlServer")]
        [TestMethod]
        public void UpdateOneInterestedColumn()
        {
            SqlTableDependency<NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel> tableDependency = null;
            string naming;

            try
            {
                var mapper = new ModelToTableMapper<NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel>();
                mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second NAME");

                tableDependency = new SqlTableDependency<NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel>(ConnectionStringForTestUser, tableName: TableName, mapper: mapper);
                tableDependency.OnChanged += TableDependency_Changed1;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent1);
                t.Start();
                Thread.Sleep(1000 * 10 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter1, 2);
            Assert.AreEqual(CheckValues1[0].Item2.Name, CheckValues1[0].Item1.Name);
            Assert.AreEqual(CheckValues1[0].Item2.Surname, CheckValues1[0].Item1.Surname);
            Assert.AreEqual(CheckValues1[1].Item2.Name, CheckValues1[1].Item1.Name);
            Assert.AreEqual(CheckValues1[1].Item2.Surname, CheckValues1[1].Item1.Surname);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming)== 0);
        }

        private static void TableDependency_Changed1(object sender, RecordChangedEventArgs<NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel> e)
        {
            if (e.ChangeType == ChangeType.Update)
            {
                CheckValues1[_counter1].Item2.Name = e.Entity.Name;
                CheckValues1[_counter1].Item2.Surname = e.Entity.Surname;
                _counter1++;
            }
        }

        private static void ModifyTableContent1()
        {
            CheckValues1.Add(new Tuple<NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel, NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel>(new NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel { Name = "Christian", Surname = "Del Bianco" }, new NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel()));
            CheckValues1.Add(new Tuple<NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel, NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel>(new NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel { Name = "Velia", Surname = "Del Bianco" }, new NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel()));

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name], [NickName]) VALUES ('xx', 'cc', 'xxxx')";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = '{CheckValues1[0].Item1.Name}', [Second Name] = '{CheckValues1[0].Item1.Surname}', [NickName] = 'xxsds'";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = '{CheckValues1[1].Item1.Name}'";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        #endregion UpdateOneInterestedColumn

        #region UpdateTwoInterestedColumn

        [TestCategory("SqlServer")]
        [TestMethod]
        public void UpdateTwoInterestedColumn()
        {
            SqlTableDependency<NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel> tableDependency = null;
            string naming;

            try
            {
                var mapper = new ModelToTableMapper<NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel>();
                mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second NAME");

                tableDependency = new SqlTableDependency<NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel>(ConnectionStringForTestUser, tableName: TableName, mapper: mapper);
                tableDependency.OnChanged += TableDependency_Changed2;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent2);
                t.Start();
                Thread.Sleep(1000 * 10 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter2, 2);
            Assert.AreEqual(CheckValues2[0].Item2.Name, CheckValues2[0].Item1.Name);
            Assert.AreEqual(CheckValues2[0].Item2.Surname, CheckValues2[0].Item1.Surname);
            Assert.AreEqual(CheckValues2[1].Item2.Name, CheckValues2[1].Item1.Name);
            Assert.AreEqual(CheckValues2[1].Item2.Surname, CheckValues2[1].Item1.Surname);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming)== 0);
        }

        private static void TableDependency_Changed2(object sender, RecordChangedEventArgs<NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel> e)
        {
            if (e.ChangeType == ChangeType.Update)
            {
                CheckValues2[_counter2].Item2.Name = e.Entity.Name;
                CheckValues2[_counter2].Item2.Surname = e.Entity.Surname;
                _counter2++;
            }
        }

        private static void ModifyTableContent2()
        {
            CheckValues2.Add(new Tuple<NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel, NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel>(new NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel { Name = "Christian", Surname = "Del Bianco" }, new NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel()));
            CheckValues2.Add(new Tuple<NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel, NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel>(new NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel { Name = "Velia", Surname = "Ceccarelli" }, new NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel()));

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name], [NickName]) VALUES ('Name', 'Surname', 'sasasa')";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = '{CheckValues2[0].Item1.Name}', [Second Name] = '{CheckValues2[0].Item1.Surname}', [NickName] = 'wswsw'";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = '{CheckValues2[1].Item1.Name}', [Second Name] = '{CheckValues2[1].Item1.Surname}', [NickName] = 'xxxx'";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        #endregion UpdateTwoInterestedColumn

        #region UpdateNoInterestedColumn

        [TestCategory("SqlServer")]
        [TestMethod]
        public void UpdateNoInterestedColumn()
        {
            SqlTableDependency<NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel> tableDependency = null;
            string naming;

            try
            {
                var mapper = new ModelToTableMapper<NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel>();
                mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second NAME");

                tableDependency = new SqlTableDependency<NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel>(ConnectionStringForTestUser, tableName: TableName, mapper: mapper);
                tableDependency.OnChanged += TableDependency_Changed3;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent3);
                t.Start();
                Thread.Sleep(1000 * 10 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter3, 0);
            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming)== 0);
        }

        private static void TableDependency_Changed3(object sender, RecordChangedEventArgs<NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServerModel> e)
        {
            if (e.ChangeType == ChangeType.Update)
            {
                _counter3++;
            }
        }

        private static void ModifyTableContent3()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name], [NickName]) VALUES ('Name', 'Surname', 'baba')";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [NickName] = 'xxxxx'";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        #endregion UpdateNoInterestedColumn
    }
}