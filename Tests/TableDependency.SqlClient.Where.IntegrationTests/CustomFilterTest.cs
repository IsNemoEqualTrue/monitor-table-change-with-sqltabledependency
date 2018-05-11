using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Abstracts;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient.BaseTests;

namespace TableDependency.SqlClient.Where.IntegrationTests
{
    public class CustomSqlTableDependencyFilter : ITableDependencyFilter
    {
        private readonly int _id;

        public CustomSqlTableDependencyFilter(int id)
        {
            _id = id;
        }

        public string Translate()
        {
            return "[Id] = " + _id;
        }
    }

    [TestClass]
    public class CustomFilterTest : SqlTableDependencyBaseTest
    {
        private class CustomFilterSqlServerModel
        {
            public int Id { get; set; }
            public string Name { get; set; }
        }

        private static readonly string TableName = typeof(CustomFilterSqlServerModel).Name;
        private static int _counter;
        private static readonly Dictionary<string, Tuple<CustomFilterSqlServerModel, CustomFilterSqlServerModel>> CheckValues = new Dictionary<string, Tuple<CustomFilterSqlServerModel, CustomFilterSqlServerModel>>();
        private static readonly Dictionary<string, Tuple<CustomFilterSqlServerModel, CustomFilterSqlServerModel>> CheckValuesOld = new Dictionary<string, Tuple<CustomFilterSqlServerModel, CustomFilterSqlServerModel>>();

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

            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<CustomFilterSqlServerModel, CustomFilterSqlServerModel>(new CustomFilterSqlServerModel { Name = "Christian" }, new CustomFilterSqlServerModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<CustomFilterSqlServerModel, CustomFilterSqlServerModel>(new CustomFilterSqlServerModel { Name = "Velia" }, new CustomFilterSqlServerModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<CustomFilterSqlServerModel, CustomFilterSqlServerModel>(new CustomFilterSqlServerModel { Name = "Velia" }, new CustomFilterSqlServerModel()));

            CheckValuesOld.Add(ChangeType.Insert.ToString(), new Tuple<CustomFilterSqlServerModel, CustomFilterSqlServerModel>(new CustomFilterSqlServerModel { Name = "Christian" }, new CustomFilterSqlServerModel()));
            CheckValuesOld.Add(ChangeType.Update.ToString(), new Tuple<CustomFilterSqlServerModel, CustomFilterSqlServerModel>(new CustomFilterSqlServerModel { Name = "Velia" }, new CustomFilterSqlServerModel()));
            CheckValuesOld.Add(ChangeType.Delete.ToString(), new Tuple<CustomFilterSqlServerModel, CustomFilterSqlServerModel>(new CustomFilterSqlServerModel { Name = "Velia" }, new CustomFilterSqlServerModel()));
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

        [TestMethod]
        public void Test()
        {
            string naming;
            SqlTableDependency<CustomFilterSqlServerModel> tableDependency = null;
            ITableDependencyFilter filterExpression = new CustomSqlTableDependencyFilter(2);

            try
            {
                tableDependency = new SqlTableDependency<CustomFilterSqlServerModel>(
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
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, CheckValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        [TestMethod]
        public void TestWithOldValues()
        {
            string naming;
            SqlTableDependency<CustomFilterSqlServerModel> tableDependency = null;
            ITableDependencyFilter filterExpression = new CustomSqlTableDependencyFilter(2);

            try
            {
                tableDependency = new SqlTableDependency<CustomFilterSqlServerModel>(
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

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item1.Name);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, CheckValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(CheckValuesOld[ChangeType.Update.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item2.Name);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<CustomFilterSqlServerModel> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;

                    if (e.EntityOldValues != null)
                    {
                        CheckValuesOld[ChangeType.Insert.ToString()].Item2.Name = e.EntityOldValues.Name;
                    }
                    else
                    {
                        CheckValuesOld[ChangeType.Insert.ToString()] = null;
                    }

                    break;

                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;

                    if (e.EntityOldValues != null)
                    {
                        CheckValuesOld[ChangeType.Update.ToString()].Item2.Name = e.EntityOldValues.Name;
                    }
                    else
                    {
                        CheckValuesOld[ChangeType.Update.ToString()] = null;
                    }

                    break;

                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;

                    if (e.EntityOldValues != null)
                    {
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
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name]) VALUES (1, 'Valentina')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name]) VALUES (2, N'{CheckValues[ChangeType.Insert.ToString()].Item1.Name}')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = 'intinazau' WHERE Id = 1";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = N'{CheckValues[ChangeType.Update.ToString()].Item1.Name}' WHERE Id = 2";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}] WHERE Id = 1";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}] WHERE Id = 2";
                    sqlCommand.ExecuteNonQuery();                    
                }
            }
        }
    }
}