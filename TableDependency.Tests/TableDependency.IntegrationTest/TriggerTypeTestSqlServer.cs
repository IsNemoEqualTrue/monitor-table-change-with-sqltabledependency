using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Exceptions;
using TableDependency.IntegrationTest.Helpers.SqlServer;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    public class TriggerTypeTestSqlServerModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
        public DateTime Born { get; set; }
        public int Quantity { get; set; }
    }

    [TestClass]
    public class TriggerTypeTestSqlServer
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServer2008 Test_User"].ConnectionString;
        private const string TableName = "CheckTriggerType";
        private static int _counter;
        private static Dictionary<string, Tuple<TriggerTypeTestSqlServerModel, TriggerTypeTestSqlServerModel>> CheckValues = new Dictionary<string, Tuple<TriggerTypeTestSqlServerModel, TriggerTypeTestSqlServerModel>>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText =
                        $"CREATE TABLE [{TableName}]( " +
                        "[Id] [int] IDENTITY(1, 1) NOT NULL, " +
                        "[Name] [NVARCHAR](50) NOT NULL, " +
                        "[Surname] [NVARCHAR](50) NOT NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize()]
        public void TestInitialize()
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }

            _counter = 0;
            CheckValues = new Dictionary<string, Tuple<TriggerTypeTestSqlServerModel, TriggerTypeTestSqlServerModel>>();
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
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
        [ExpectedException(typeof(DmlTriggerTypeException))]
        public void RaiseException1()
        {
            SqlTableDependency<TriggerTypeTestSqlServerModel> tableDependency = null;
            string naming = null;

            var updateOf = new UpdateOfModel<TriggerTypeTestSqlServerModel>();
            updateOf.Add(i => i.Surname);

            try
            {
                tableDependency = new SqlTableDependency<TriggerTypeTestSqlServerModel>(
                    ConnectionString,
                    tableName: TableName,
                    updateOf: updateOf, 
                    notifyOn: DmlTriggerType.Insert);

                naming = tableDependency.DataBaseObjectsNamingConvention;
            }
            finally
            {
                tableDependency?.Dispose();
            }
           
            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(naming));
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        [ExpectedException(typeof(DmlTriggerTypeException))]
        public void RaiseException2()
        {
            var updateOf = new UpdateOfModel<TriggerTypeTestSqlServerModel>();
            updateOf.Add(t => t.Surname);

            SqlTableDependency<TriggerTypeTestSqlServerModel> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<TriggerTypeTestSqlServerModel>(
                    ConnectionString,
                    tableName: TableName,
                    updateOf: updateOf,
                    notifyOn: DmlTriggerType.Delete);

                naming = tableDependency.DataBaseObjectsNamingConvention;
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(naming));
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        [ExpectedException(typeof(DmlTriggerTypeException))]
        public void RaiseException3()
        {
            var updateOf = new UpdateOfModel<TriggerTypeTestSqlServerModel>();
            updateOf.Add(t => t.Surname);

            SqlTableDependency<TriggerTypeTestSqlServerModel> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<TriggerTypeTestSqlServerModel>(
                    ConnectionString,
                    tableName: TableName,
                    updateOf: updateOf,
                    notifyOn: DmlTriggerType.Delete | DmlTriggerType.Insert);

                naming = tableDependency.DataBaseObjectsNamingConvention;
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(naming));
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void OnlyInsert()
        {
            SqlTableDependency<TriggerTypeTestSqlServerModel> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<TriggerTypeTestSqlServerModel>(
                    ConnectionString,
                    tableName: TableName,
                    notifyOn : DmlTriggerType.Insert);

                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 1);

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, "Pizza Mergherita");
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(naming));
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void OnlyDelete()
        {
            SqlTableDependency<TriggerTypeTestSqlServerModel> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<TriggerTypeTestSqlServerModel>(
                    ConnectionString,
                    tableName: TableName,
                    notifyOn: DmlTriggerType.Delete);

                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 1);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(naming));
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void OnlyUdate()
        {
            SqlTableDependency<TriggerTypeTestSqlServerModel> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<TriggerTypeTestSqlServerModel>(
                    ConnectionString,
                    tableName: TableName,
                    notifyOn: DmlTriggerType.Update);

                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 1);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(naming));
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void OnlyInsertDelete()
        {
            SqlTableDependency<TriggerTypeTestSqlServerModel> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<TriggerTypeTestSqlServerModel>(
                    ConnectionString,
                    tableName: TableName,
                    notifyOn: DmlTriggerType.Insert | DmlTriggerType.Delete);

                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 2);

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, "Pizza Mergherita");
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(naming));
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void OnlyInsertUpdate()
        {
            SqlTableDependency<TriggerTypeTestSqlServerModel> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<TriggerTypeTestSqlServerModel>(
                    ConnectionString,
                    tableName: TableName,
                    notifyOn: DmlTriggerType.Insert | DmlTriggerType.Update);

                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 2);

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, "Pizza Mergherita");
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(naming));
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void DeleteInsertUpdate()
        {
            SqlTableDependency<TriggerTypeTestSqlServerModel> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<TriggerTypeTestSqlServerModel>(
                    ConnectionString,
                    tableName: TableName,
                    notifyOn: DmlTriggerType.Delete | DmlTriggerType.Insert | DmlTriggerType.Update);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, "Pizza Mergherita");
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(naming));
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void All()
        {
            SqlTableDependency<TriggerTypeTestSqlServerModel> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<TriggerTypeTestSqlServerModel>(
                    ConnectionString,
                    tableName: TableName,
                    notifyOn: DmlTriggerType.All);

                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, "Pizza Mergherita");
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(naming));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<TriggerTypeTestSqlServerModel> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Surname = e.Entity.Surname;
                    break;
                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Surname = e.Entity.Surname;
                    break;
                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Update.ToString()].Item2.Surname = e.Entity.Surname;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<TriggerTypeTestSqlServerModel, TriggerTypeTestSqlServerModel>(new TriggerTypeTestSqlServerModel { Id = 23, Name = "Pizza Mergherita", Surname = "Pizza Mergherita" }, new TriggerTypeTestSqlServerModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<TriggerTypeTestSqlServerModel, TriggerTypeTestSqlServerModel>(new TriggerTypeTestSqlServerModel { Id = 23, Name = "Pizza Funghi", Surname = "Pizza Mergherita" }, new TriggerTypeTestSqlServerModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<TriggerTypeTestSqlServerModel, TriggerTypeTestSqlServerModel>(new TriggerTypeTestSqlServerModel { Id = 23, Name = "Pizza Funghi", Surname = "Pizza Funghi" }, new TriggerTypeTestSqlServerModel()));

            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name], [Surname]) VALUES ('{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', '{CheckValues[ChangeType.Insert.ToString()].Item1.Surname}')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = '{CheckValues[ChangeType.Update.ToString()].Item1.Name}'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);
                }
            }
        }
    }
}