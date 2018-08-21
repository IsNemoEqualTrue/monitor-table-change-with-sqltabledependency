using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;
using TableDependency.SqlClient.Test.Base;
using TableDependency.SqlClient.Where;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class WhereEqualToTest : SqlTableDependencyBaseTest
    {
        private class EqualToTestSqlServerModel
        {
            public int Id { get; set; }
            public string Name { get; set; }
        }

        // Cannot be static !!!
        private const int _id = 2;
        private static readonly string TableName = typeof(EqualToTestSqlServerModel).Name;
        private static int _counter;
        private static readonly Dictionary<string, Tuple<EqualToTestSqlServerModel, EqualToTestSqlServerModel>> CheckValues = new Dictionary<string, Tuple<EqualToTestSqlServerModel, EqualToTestSqlServerModel>>();
        private static readonly Dictionary<string, Tuple<EqualToTestSqlServerModel, EqualToTestSqlServerModel>> CheckValuesOld = new Dictionary<string, Tuple<EqualToTestSqlServerModel, EqualToTestSqlServerModel>>();

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID(N'{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText =
                        $"CREATE TABLE [{TableName}]( " +
                        "[Id] [int] NOT NULL, " +
                        "[Name] [nvarchar](50) NOT NULL, " +
                        "[Second Name] [nvarchar](50) NULL, " +
                        "[Born] [datetime] NULL)";
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

            _counter = 0;

            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<EqualToTestSqlServerModel, EqualToTestSqlServerModel>(new EqualToTestSqlServerModel { Id = _id, Name = "Christian" }, new EqualToTestSqlServerModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<EqualToTestSqlServerModel, EqualToTestSqlServerModel>(new EqualToTestSqlServerModel { Id = _id, Name = "Velia" }, new EqualToTestSqlServerModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<EqualToTestSqlServerModel, EqualToTestSqlServerModel>(new EqualToTestSqlServerModel { Id = _id, Name = "Velia" }, new EqualToTestSqlServerModel()));

            CheckValuesOld.Add(ChangeType.Insert.ToString(), new Tuple<EqualToTestSqlServerModel, EqualToTestSqlServerModel>(new EqualToTestSqlServerModel { Id = _id, Name = "Christian" }, new EqualToTestSqlServerModel()));
            CheckValuesOld.Add(ChangeType.Update.ToString(), new Tuple<EqualToTestSqlServerModel, EqualToTestSqlServerModel>(new EqualToTestSqlServerModel { Id = _id, Name = "Velia" }, new EqualToTestSqlServerModel()));
            CheckValuesOld.Add(ChangeType.Delete.ToString(), new Tuple<EqualToTestSqlServerModel, EqualToTestSqlServerModel>(new EqualToTestSqlServerModel { Id = _id, Name = "Velia" }, new EqualToTestSqlServerModel()));
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID(N'{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestMethod]
        public void Test()
        {
            SqlTableDependency<EqualToTestSqlServerModel> tableDependency = null;
            string naming;

            Expression<Func<EqualToTestSqlServerModel, bool>> expression = p => p.Id == _id;
            var filterExpression = new SqlTableDependencyFilter<EqualToTestSqlServerModel>(expression);

            try
            {
                tableDependency = new SqlTableDependency<EqualToTestSqlServerModel>(
                    ConnectionStringForTestUser,
                    includeOldValues: true,
                    filter: filterExpression);

                tableDependency.OnChanged += TableDependency_Changed;
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

            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Id, CheckValues[ChangeType.Insert.ToString()].Item1.Id);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item1.Name);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Id, CheckValues[ChangeType.Update.ToString()].Item1.Id);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, CheckValues[ChangeType.Update.ToString()].Item1.Name);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Id, CheckValues[ChangeType.Delete.ToString()].Item1.Id);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        [TestMethod]
        public void TestWithOldValues()
        {
            SqlTableDependency<EqualToTestSqlServerModel> tableDependency = null;
            string naming;

            Expression<Func<EqualToTestSqlServerModel, bool>> expression = p => p.Id == _id;
            var filterExpression = new SqlTableDependencyFilter<EqualToTestSqlServerModel>(expression);

            try
            {
                tableDependency = new SqlTableDependency<EqualToTestSqlServerModel>(
                    ConnectionStringForTestUser,
                    includeOldValues: true,
                    filter: filterExpression);

                tableDependency.OnChanged += TableDependency_Changed;
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

            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Id, CheckValues[ChangeType.Insert.ToString()].Item1.Id);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.IsNull(CheckValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Id, CheckValues[ChangeType.Update.ToString()].Item1.Id);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, CheckValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(CheckValuesOld[ChangeType.Update.ToString()].Item2.Id, CheckValues[ChangeType.Insert.ToString()].Item2.Id);
            Assert.AreEqual(CheckValuesOld[ChangeType.Update.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item2.Name);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Id, CheckValues[ChangeType.Delete.ToString()].Item1.Id);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.IsNull(CheckValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<EqualToTestSqlServerModel> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.Id = e.Entity.Id;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;

                    if (e.EntityOldValues != null)
                    {
                        CheckValuesOld[ChangeType.Insert.ToString()].Item2.Id = e.EntityOldValues.Id;
                        CheckValuesOld[ChangeType.Insert.ToString()].Item2.Name = e.EntityOldValues.Name;
                    }
                    else
                    {
                        CheckValuesOld[ChangeType.Insert.ToString()] = null;
                    }

                    break;

                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.Id = e.Entity.Id;
                    CheckValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;

                    if (e.EntityOldValues != null)
                    {
                        CheckValuesOld[ChangeType.Update.ToString()].Item2.Id = e.EntityOldValues.Id;
                        CheckValuesOld[ChangeType.Update.ToString()].Item2.Name = e.EntityOldValues.Name;
                    }
                    else
                    {
                        CheckValuesOld[ChangeType.Update.ToString()] = null;
                    }

                    break;

                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.Id = e.Entity.Id;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;

                    if (e.EntityOldValues != null)
                    {
                        CheckValuesOld[ChangeType.Delete.ToString()].Item2.Id = e.EntityOldValues.Id;
                        CheckValuesOld[ChangeType.Delete.ToString()].Item2.Name = e.EntityOldValues.Name;
                    }
                    else
                    {
                        CheckValuesOld[ChangeType.Delete.ToString()] = null;
                    }

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
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name]) VALUES (999, N'Iron Man')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name]) VALUES ({CheckValues[ChangeType.Insert.ToString()].Item1.Id}, N'{CheckValues[ChangeType.Insert.ToString()].Item1.Name}')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = N'Spider Man' WHERE [Id] = 999";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = N'{CheckValues[ChangeType.Update.ToString()].Item1.Name}' WHERE [Id] = {CheckValues[ChangeType.Update.ToString()].Item1.Id}";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}] WHERE [Id]= 999";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}] WHERE [Id] = {CheckValues[ChangeType.Delete.ToString()].Item1.Id}";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}