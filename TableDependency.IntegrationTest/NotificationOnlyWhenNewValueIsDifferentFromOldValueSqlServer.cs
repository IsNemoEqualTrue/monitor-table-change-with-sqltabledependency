using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.SqlServer;
using TableDependency.Mappers;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    public class ABCTableModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
    }

    [TestClass]
    public class NotificationOnlyWhenNewValueIsDifferentFromOldValueSqlServer
    {
        private static readonly string _connectionString = ConfigurationManager.ConnectionStrings["SqlServerConnectionString"].ConnectionString;
        private const string TableName = "ABCTableModel";
        private static int _counter1;
        private static int _counter2;
        private static int _counter3;
        private static List<Tuple<ABCTableModel, ABCTableModel>> _checkValues1 = new List<Tuple<ABCTableModel, ABCTableModel>>();
        private static List<Tuple<ABCTableModel, ABCTableModel>> _checkValues2 = new List<Tuple<ABCTableModel, ABCTableModel>>();
        private static List<Tuple<ABCTableModel, ABCTableModel>> _checkValues3 = new List<Tuple<ABCTableModel, ABCTableModel>>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
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
            using (var sqlConnection = new SqlConnection(_connectionString))
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
            SqlTableDependency<ABCTableModel> tableDependency = null;
            string naming = null;

            try
            {
                var mapper = new ModelToTableMapper<ABCTableModel>();
                mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second NAME");

                tableDependency = new SqlTableDependency<ABCTableModel>(_connectionString, TableName, mapper);
                tableDependency.OnChanged += TableDependency_Changed1;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent1);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter1, 2);
            Assert.AreEqual(_checkValues1[0].Item2.Name, _checkValues1[0].Item1.Name);
            Assert.AreEqual(_checkValues1[0].Item2.Surname, _checkValues1[0].Item1.Surname);
            Assert.AreEqual(_checkValues1[1].Item2.Name, _checkValues1[1].Item1.Name);
            Assert.AreEqual(_checkValues1[1].Item2.Surname, _checkValues1[1].Item1.Surname);
            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(_connectionString, naming));
        }

        private static void TableDependency_Changed1(object sender, RecordChangedEventArgs<ABCTableModel> e)
        {
            if (e.ChangeType == ChangeType.Update)
            {
                _checkValues1[_counter1].Item2.Name = e.Entity.Name;
                _checkValues1[_counter1].Item2.Surname = e.Entity.Surname;
                _counter1++;
            }
        }

        private static void ModifyTableContent1()
        {
            _checkValues1.Add(new Tuple<ABCTableModel, ABCTableModel>(new ABCTableModel { Name = "Christian", Surname = "Del Bianco" }, new ABCTableModel()));
            _checkValues1.Add(new Tuple<ABCTableModel, ABCTableModel>(new ABCTableModel { Name = "Velia", Surname = "Del Bianco" }, new ABCTableModel()));

            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name], [NickName]) VALUES ('xx', 'cc', 'xxxx')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = '{_checkValues1[0].Item1.Name}', [Second Name] = '{_checkValues1[0].Item1.Surname}', [NickName] = 'xxsds'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = '{_checkValues1[1].Item1.Name}'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);
                }
            }
        }

        #endregion UpdateOneInterestedColumn

        #region UpdateTwoInterestedColumn

        [TestCategory("SqlServer")]
        [TestMethod]
        public void UpdateTwoInterestedColumn()
        {
            SqlTableDependency<ABCTableModel> tableDependency = null;
            string naming = null;

            try
            {
                var mapper = new ModelToTableMapper<ABCTableModel>();
                mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second NAME");

                tableDependency = new SqlTableDependency<ABCTableModel>(_connectionString, TableName, mapper);
                tableDependency.OnChanged += TableDependency_Changed2;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent2);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter2, 2);
            Assert.AreEqual(_checkValues2[0].Item2.Name, _checkValues2[0].Item1.Name);
            Assert.AreEqual(_checkValues2[0].Item2.Surname, _checkValues2[0].Item1.Surname);
            Assert.AreEqual(_checkValues2[1].Item2.Name, _checkValues2[1].Item1.Name);
            Assert.AreEqual(_checkValues2[1].Item2.Surname, _checkValues2[1].Item1.Surname);
            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(_connectionString, naming));
        }

        private static void TableDependency_Changed2(object sender, RecordChangedEventArgs<ABCTableModel> e)
        {
            if (e.ChangeType == ChangeType.Update)
            {
                _checkValues2[_counter2].Item2.Name = e.Entity.Name;
                _checkValues2[_counter2].Item2.Surname = e.Entity.Surname;
                _counter2++;
            }
        }

        private static void ModifyTableContent2()
        {
            _checkValues2.Add(new Tuple<ABCTableModel, ABCTableModel>(new ABCTableModel { Name = "Christian", Surname = "Del Bianco" }, new ABCTableModel()));
            _checkValues2.Add(new Tuple<ABCTableModel, ABCTableModel>(new ABCTableModel { Name = "Velia", Surname = "Ceccarelli" }, new ABCTableModel()));

            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name], [NickName]) VALUES ('Name', 'Surname', 'sasasa')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = '{_checkValues2[0].Item1.Name}', [Second Name] = '{_checkValues2[0].Item1.Surname}', [NickName] = 'wswsw'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = '{_checkValues2[1].Item1.Name}', [Second Name] = '{_checkValues2[1].Item1.Surname}', [NickName] = 'xxxx'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);
                }
            }
        }

        #endregion UpdateTwoInterestedColumn

        #region UpdateNoInterestedColumn

        [TestCategory("SqlServer")]
        [TestMethod]
        public void UpdateNoInterestedColumn()
        {
            SqlTableDependency<ABCTableModel> tableDependency = null;
            string naming = null;

            try
            {
                var mapper = new ModelToTableMapper<ABCTableModel>();
                mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second NAME");

                tableDependency = new SqlTableDependency<ABCTableModel>(_connectionString, TableName, mapper);
                tableDependency.OnChanged += TableDependency_Changed3;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent3);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter3, 0);
            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(_connectionString, naming));
        }

        private static void TableDependency_Changed3(object sender, RecordChangedEventArgs<ABCTableModel> e)
        {
            if (e.ChangeType == ChangeType.Update)
            {
                _counter3++;
            }
        }

        private static void ModifyTableContent3()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name], [NickName]) VALUES ('Name', 'Surname', 'baba')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [NickName] = 'xxxxx'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);
                }
            }
        }

        #endregion UpdateNoInterestedColumn
    }
}