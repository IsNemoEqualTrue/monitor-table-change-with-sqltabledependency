using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Base;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class DataAnnotationTest02 : Base.SqlTableDependencyBaseTest
    {
        [Table("ANItemsTableSQL2")]
        private class DataAnnotationTestSqlServer2Model
        {
            public long Id { get; set; }

            public string Name { get; set; }

            [Column("Long")]
            public string Description { get; set; }
        }

        private const string TableName = "ANItemsTableSQL2";
        private static int _counter;
        private static Dictionary<string, Tuple<DataAnnotationTestSqlServer2Model, DataAnnotationTestSqlServer2Model>> _checkValues = new Dictionary<string, Tuple<DataAnnotationTestSqlServer2Model, DataAnnotationTestSqlServer2Model>>();
        private static Dictionary<string, Tuple<DataAnnotationTestSqlServer2Model, DataAnnotationTestSqlServer2Model>> _checkValuesOld = new Dictionary<string, Tuple<DataAnnotationTestSqlServer2Model, DataAnnotationTestSqlServer2Model>>();

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

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [int] IDENTITY(1, 1) NOT NULL, [Name] [NVARCHAR](50) NULL, [Long Description] [NVARCHAR](MAX) NULL)";
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

            _counter = 0;

            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<DataAnnotationTestSqlServer2Model, DataAnnotationTestSqlServer2Model>(new DataAnnotationTestSqlServer2Model { Name = "Christian", Description = "Del Bianco" }, new DataAnnotationTestSqlServer2Model()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<DataAnnotationTestSqlServer2Model, DataAnnotationTestSqlServer2Model>(new DataAnnotationTestSqlServer2Model { Name = "Velia", Description = "Ceccarelli" }, new DataAnnotationTestSqlServer2Model()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<DataAnnotationTestSqlServer2Model, DataAnnotationTestSqlServer2Model>(new DataAnnotationTestSqlServer2Model { Name = "Velia", Description = "Ceccarelli" }, new DataAnnotationTestSqlServer2Model()));

            _checkValuesOld.Add(ChangeType.Insert.ToString(), new Tuple<DataAnnotationTestSqlServer2Model, DataAnnotationTestSqlServer2Model>(new DataAnnotationTestSqlServer2Model { Name = "Christian", Description = "Del Bianco" }, new DataAnnotationTestSqlServer2Model()));
            _checkValuesOld.Add(ChangeType.Update.ToString(), new Tuple<DataAnnotationTestSqlServer2Model, DataAnnotationTestSqlServer2Model>(new DataAnnotationTestSqlServer2Model { Name = "Velia", Description = "Ceccarelli" }, new DataAnnotationTestSqlServer2Model()));
            _checkValuesOld.Add(ChangeType.Delete.ToString(), new Tuple<DataAnnotationTestSqlServer2Model, DataAnnotationTestSqlServer2Model>(new DataAnnotationTestSqlServer2Model { Name = "Velia", Description = "Ceccarelli" }, new DataAnnotationTestSqlServer2Model()));
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
            SqlTableDependency<DataAnnotationTestSqlServer2Model> tableDependency = null;
            string naming;

            var mapper = new ModelToTableMapper<DataAnnotationTestSqlServer2Model>();
            mapper.AddMapping(c => c.Description, "Long Description");

            try
            {
                tableDependency = new SqlTableDependency<DataAnnotationTestSqlServer2Model>(ConnectionStringForTestUser, mapper: mapper);
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

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Name, _checkValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Description, _checkValues[ChangeType.Insert.ToString()].Item1.Description);
            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Name, _checkValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Description, _checkValues[ChangeType.Update.ToString()].Item1.Description);
            Assert.IsNull(_checkValuesOld[ChangeType.Update.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Name, _checkValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Description, _checkValues[ChangeType.Delete.ToString()].Item1.Description);
            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TestWithOldValues()
        {
            SqlTableDependency<DataAnnotationTestSqlServer2Model> tableDependency = null;
            string naming;

            var mapper = new ModelToTableMapper<DataAnnotationTestSqlServer2Model>();
            mapper.AddMapping(c => c.Description, "Long Description");

            try
            {
                tableDependency = new SqlTableDependency<DataAnnotationTestSqlServer2Model>(ConnectionStringForTestUser, includeOldValues: true, mapper: mapper);
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

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Name, _checkValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Description, _checkValues[ChangeType.Insert.ToString()].Item1.Description);
            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Name, _checkValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Description, _checkValues[ChangeType.Update.ToString()].Item1.Description);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.Name, _checkValues[ChangeType.Insert.ToString()].Item2.Name);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.Description, _checkValues[ChangeType.Insert.ToString()].Item2.Description);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Name, _checkValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Description, _checkValues[ChangeType.Delete.ToString()].Item1.Description);
            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<DataAnnotationTestSqlServer2Model> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    _checkValues[ChangeType.Insert.ToString()].Item2.Description = e.Entity.Description;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.Name = e.EntityOldValues.Name;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.Description = e.EntityOldValues.Description;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()] = null;
                    }

                    break;

                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                    _checkValues[ChangeType.Update.ToString()].Item2.Description = e.Entity.Description;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.Name = e.EntityOldValues.Name;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.Description = e.EntityOldValues.Description;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Update.ToString()] = null;
                    }

                    break;

                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    _checkValues[ChangeType.Delete.ToString()].Item2.Description = e.Entity.Description;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.Name = e.EntityOldValues.Name;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.Description = e.EntityOldValues.Description;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Delete.ToString()] = null;
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
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name], [Long Description]) VALUES ('{_checkValues[ChangeType.Insert.ToString()].Item1.Name}', '{_checkValues[ChangeType.Insert.ToString()].Item1.Description}')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = '{_checkValues[ChangeType.Update.ToString()].Item1.Name}', [Long Description] = '{_checkValues[ChangeType.Update.ToString()].Item1.Description}'";
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