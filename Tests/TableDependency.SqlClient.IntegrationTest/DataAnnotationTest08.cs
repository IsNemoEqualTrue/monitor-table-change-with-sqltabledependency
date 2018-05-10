using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Exceptions;
using TableDependency.SqlClient.BaseTests;

namespace TableDependency.SqlClient.IntegrationTests
{
    public class DataAnnotationTestSqlServer8Model
    {
        public long IdNotExist { get; set; }
        public string NameNotExist { get; set; }
        public string DescriptionNotExist { get; set; }
    }

    [TestClass]
    public class DataAnnotationTest08 : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(DataAnnotationTestSqlServer8Model).Name;
        private static int _counter;
        private static Dictionary<string, Tuple<DataAnnotationTestSqlServer8Model, DataAnnotationTestSqlServer8Model>> _checkValues = new Dictionary<string, Tuple<DataAnnotationTestSqlServer8Model, DataAnnotationTestSqlServer8Model>>();
        private static Dictionary<string, Tuple<DataAnnotationTestSqlServer8Model, DataAnnotationTestSqlServer8Model>> _checkValuesOld = new Dictionary<string, Tuple<DataAnnotationTestSqlServer8Model, DataAnnotationTestSqlServer8Model>>();

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

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [int] IDENTITY(1, 1) NOT NULL, [Name] [NVARCHAR](50) NULL, [Long Description] [NVARCHAR](MAX) NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize()]
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

            _counter = 0;

            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<DataAnnotationTestSqlServer8Model, DataAnnotationTestSqlServer8Model>(new DataAnnotationTestSqlServer8Model { NameNotExist = "Christian", DescriptionNotExist = "Del Bianco" }, new DataAnnotationTestSqlServer8Model()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<DataAnnotationTestSqlServer8Model, DataAnnotationTestSqlServer8Model>(new DataAnnotationTestSqlServer8Model { NameNotExist = "Velia", DescriptionNotExist = "Ceccarelli" }, new DataAnnotationTestSqlServer8Model()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<DataAnnotationTestSqlServer8Model, DataAnnotationTestSqlServer8Model>(new DataAnnotationTestSqlServer8Model { NameNotExist = "Velia", DescriptionNotExist = "Ceccarelli" }, new DataAnnotationTestSqlServer8Model()));

            _checkValuesOld.Add(ChangeType.Insert.ToString(), new Tuple<DataAnnotationTestSqlServer8Model, DataAnnotationTestSqlServer8Model>(new DataAnnotationTestSqlServer8Model { NameNotExist = "Christian", DescriptionNotExist = "Del Bianco" }, new DataAnnotationTestSqlServer8Model()));
            _checkValuesOld.Add(ChangeType.Update.ToString(), new Tuple<DataAnnotationTestSqlServer8Model, DataAnnotationTestSqlServer8Model>(new DataAnnotationTestSqlServer8Model { NameNotExist = "Velia", DescriptionNotExist = "Ceccarelli" }, new DataAnnotationTestSqlServer8Model()));
            _checkValuesOld.Add(ChangeType.Delete.ToString(), new Tuple<DataAnnotationTestSqlServer8Model, DataAnnotationTestSqlServer8Model>(new DataAnnotationTestSqlServer8Model { NameNotExist = "Velia", DescriptionNotExist = "Ceccarelli" }, new DataAnnotationTestSqlServer8Model()));
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
        [ExpectedException(typeof(NoMatchBetweenModelAndTableColumns))]
        public void Test()
        {
            SqlTableDependency<DataAnnotationTestSqlServer8Model> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<DataAnnotationTestSqlServer8Model>(ConnectionStringForTestUser);
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

            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.NameNotExist, _checkValues[ChangeType.Insert.ToString()].Item1.NameNotExist);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.DescriptionNotExist, _checkValues[ChangeType.Insert.ToString()].Item1.DescriptionNotExist);
            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.NameNotExist, _checkValues[ChangeType.Update.ToString()].Item1.NameNotExist);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.DescriptionNotExist, _checkValues[ChangeType.Update.ToString()].Item1.DescriptionNotExist);
            Assert.IsNull(_checkValuesOld[ChangeType.Update.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.NameNotExist, _checkValues[ChangeType.Delete.ToString()].Item1.NameNotExist);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.DescriptionNotExist, _checkValues[ChangeType.Delete.ToString()].Item1.DescriptionNotExist);
            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        [ExpectedException(typeof(NoMatchBetweenModelAndTableColumns))]
        public void TestWithOldValues()
        {
            SqlTableDependency<DataAnnotationTestSqlServer8Model> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<DataAnnotationTestSqlServer8Model>(ConnectionStringForTestUser, includeOldValues: true);
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

            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.NameNotExist, _checkValues[ChangeType.Insert.ToString()].Item1.NameNotExist);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.DescriptionNotExist, _checkValues[ChangeType.Insert.ToString()].Item1.DescriptionNotExist);
            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.NameNotExist, _checkValues[ChangeType.Update.ToString()].Item1.NameNotExist);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.DescriptionNotExist, _checkValues[ChangeType.Update.ToString()].Item1.DescriptionNotExist);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.NameNotExist, _checkValues[ChangeType.Insert.ToString()].Item2.NameNotExist);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.DescriptionNotExist, _checkValues[ChangeType.Insert.ToString()].Item2.DescriptionNotExist);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.NameNotExist, _checkValues[ChangeType.Delete.ToString()].Item1.NameNotExist);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.DescriptionNotExist, _checkValues[ChangeType.Delete.ToString()].Item1.DescriptionNotExist);
            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<DataAnnotationTestSqlServer8Model> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.NameNotExist = e.Entity.NameNotExist;
                    _checkValues[ChangeType.Insert.ToString()].Item2.DescriptionNotExist = e.Entity.DescriptionNotExist;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.NameNotExist = e.EntityOldValues.NameNotExist;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.DescriptionNotExist = e.EntityOldValues.DescriptionNotExist;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()] = null;
                    }

                    break;

                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.NameNotExist = e.Entity.NameNotExist;
                    _checkValues[ChangeType.Update.ToString()].Item2.DescriptionNotExist = e.Entity.DescriptionNotExist;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.NameNotExist = e.EntityOldValues.NameNotExist;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.DescriptionNotExist = e.EntityOldValues.DescriptionNotExist;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()] = null;
                    }

                    break;

                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.NameNotExist = e.Entity.NameNotExist;
                    _checkValues[ChangeType.Delete.ToString()].Item2.DescriptionNotExist = e.Entity.DescriptionNotExist;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.NameNotExist = e.EntityOldValues.NameNotExist;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.DescriptionNotExist = e.EntityOldValues.DescriptionNotExist;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()] = null;
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
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name], [Long Description]) VALUES ('{_checkValues[ChangeType.Insert.ToString()].Item1.NameNotExist}', '{_checkValues[ChangeType.Insert.ToString()].Item1.DescriptionNotExist}')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = '{_checkValues[ChangeType.Update.ToString()].Item1.NameNotExist}', [Long Description] = '{_checkValues[ChangeType.Update.ToString()].Item1.DescriptionNotExist}'";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}