using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient.BaseTests;

namespace TableDependency.SqlClient.IntegrationTests
{
    public class BigIntDecimalAndFloatTypesTestSqlServerModel
    {
        public long? BigintColumn { get; set; }
        public decimal? Decimal18Column { get; set; }
        public decimal? Decimal54Column { get; set; }
        public float? FloatColumn { get; set; }
    }

    [TestClass]
    public class BigIntDecimalAndFloatTypesTest : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(BigIntDecimalAndFloatTypesTestSqlServerModel).Name;
        private static Dictionary<string, Tuple<BigIntDecimalAndFloatTypesTestSqlServerModel, BigIntDecimalAndFloatTypesTestSqlServerModel>> _checkValues = new Dictionary<string, Tuple<BigIntDecimalAndFloatTypesTestSqlServerModel, BigIntDecimalAndFloatTypesTestSqlServerModel>>();
        private static Dictionary<string, Tuple<BigIntDecimalAndFloatTypesTestSqlServerModel, BigIntDecimalAndFloatTypesTestSqlServerModel>> _checkValuesOld = new Dictionary<string, Tuple<BigIntDecimalAndFloatTypesTestSqlServerModel, BigIntDecimalAndFloatTypesTestSqlServerModel>>();

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForSa))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"CREATE TABLE {TableName}(" +
                        "BigintColumn BIGINT NULL," +
                        "Decimal18Column DECIMAL(18, 0) NULL, " +
                        "Decimal54Column DECIMAL(5, 4) NULL, " +
                        "FloatColumn FLOAT NULL)";
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

            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<BigIntDecimalAndFloatTypesTestSqlServerModel, BigIntDecimalAndFloatTypesTestSqlServerModel>(new BigIntDecimalAndFloatTypesTestSqlServerModel { BigintColumn = 123, Decimal18Column = 987654321, Decimal54Column = null, FloatColumn = null }, new BigIntDecimalAndFloatTypesTestSqlServerModel()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<BigIntDecimalAndFloatTypesTestSqlServerModel, BigIntDecimalAndFloatTypesTestSqlServerModel>(new BigIntDecimalAndFloatTypesTestSqlServerModel { BigintColumn = null, Decimal18Column = null, Decimal54Column = 6.77M, FloatColumn = 7.55F }, new BigIntDecimalAndFloatTypesTestSqlServerModel()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<BigIntDecimalAndFloatTypesTestSqlServerModel, BigIntDecimalAndFloatTypesTestSqlServerModel>(new BigIntDecimalAndFloatTypesTestSqlServerModel { BigintColumn = null, Decimal18Column = null, Decimal54Column = 6.77M, FloatColumn = 7.55F }, new BigIntDecimalAndFloatTypesTestSqlServerModel()));

            _checkValuesOld.Add(ChangeType.Insert.ToString(), new Tuple<BigIntDecimalAndFloatTypesTestSqlServerModel, BigIntDecimalAndFloatTypesTestSqlServerModel>(new BigIntDecimalAndFloatTypesTestSqlServerModel { BigintColumn = 123, Decimal18Column = 987654321, Decimal54Column = null, FloatColumn = null }, new BigIntDecimalAndFloatTypesTestSqlServerModel()));
            _checkValuesOld.Add(ChangeType.Update.ToString(), new Tuple<BigIntDecimalAndFloatTypesTestSqlServerModel, BigIntDecimalAndFloatTypesTestSqlServerModel>(new BigIntDecimalAndFloatTypesTestSqlServerModel { BigintColumn = null, Decimal18Column = null, Decimal54Column = 6.77M, FloatColumn = 7.55F }, new BigIntDecimalAndFloatTypesTestSqlServerModel()));
            _checkValuesOld.Add(ChangeType.Delete.ToString(), new Tuple<BigIntDecimalAndFloatTypesTestSqlServerModel, BigIntDecimalAndFloatTypesTestSqlServerModel>(new BigIntDecimalAndFloatTypesTestSqlServerModel { BigintColumn = null, Decimal18Column = null, Decimal54Column = 6.77M, FloatColumn = 7.55F }, new BigIntDecimalAndFloatTypesTestSqlServerModel()));
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForSa))
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
            SqlTableDependency<BigIntDecimalAndFloatTypesTestSqlServerModel> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<BigIntDecimalAndFloatTypesTestSqlServerModel>(ConnectionStringForTestUser);
                tableDependency.OnChanged += this.TableDependency_Changed;
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

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.BigintColumn, _checkValues[ChangeType.Insert.ToString()].Item1.BigintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Decimal18Column, _checkValues[ChangeType.Insert.ToString()].Item1.Decimal18Column);
            Assert.IsNull(_checkValues[ChangeType.Insert.ToString()].Item2.Decimal54Column);
            Assert.IsNull(_checkValues[ChangeType.Insert.ToString()].Item2.FloatColumn);

            Assert.IsNull(_checkValues[ChangeType.Update.ToString()].Item2.BigintColumn);
            Assert.IsNull(_checkValues[ChangeType.Update.ToString()].Item2.Decimal18Column);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Decimal54Column, _checkValues[ChangeType.Update.ToString()].Item1.Decimal54Column);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.FloatColumn, _checkValues[ChangeType.Update.ToString()].Item1.FloatColumn);

            Assert.IsNull(_checkValues[ChangeType.Delete.ToString()].Item2.BigintColumn);
            Assert.IsNull(_checkValues[ChangeType.Delete.ToString()].Item2.Decimal18Column);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Decimal54Column, _checkValues[ChangeType.Delete.ToString()].Item1.Decimal54Column);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.FloatColumn, _checkValues[ChangeType.Delete.ToString()].Item1.FloatColumn);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TestWithOldValues()
        {
            SqlTableDependency<BigIntDecimalAndFloatTypesTestSqlServerModel> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<BigIntDecimalAndFloatTypesTestSqlServerModel>(ConnectionStringForTestUser, includeOldValues: true);
                tableDependency.OnChanged += this.TableDependency_Changed;
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

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.BigintColumn, _checkValues[ChangeType.Insert.ToString()].Item1.BigintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Decimal18Column, _checkValues[ChangeType.Insert.ToString()].Item1.Decimal18Column);
            Assert.IsNull(_checkValues[ChangeType.Insert.ToString()].Item2.Decimal54Column);
            Assert.IsNull(_checkValues[ChangeType.Insert.ToString()].Item2.FloatColumn);

            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.IsNull(_checkValues[ChangeType.Update.ToString()].Item2.BigintColumn);
            Assert.IsNull(_checkValues[ChangeType.Update.ToString()].Item2.Decimal18Column);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Decimal54Column, _checkValues[ChangeType.Update.ToString()].Item1.Decimal54Column);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.FloatColumn, _checkValues[ChangeType.Update.ToString()].Item1.FloatColumn);

            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.BigintColumn, _checkValues[ChangeType.Insert.ToString()].Item2.BigintColumn);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.Decimal18Column, _checkValues[ChangeType.Insert.ToString()].Item2.Decimal18Column);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.Decimal54Column, _checkValues[ChangeType.Insert.ToString()].Item2.Decimal54Column);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.FloatColumn, _checkValues[ChangeType.Insert.ToString()].Item2.FloatColumn);

            Assert.IsNull(_checkValues[ChangeType.Delete.ToString()].Item2.BigintColumn);
            Assert.IsNull(_checkValues[ChangeType.Delete.ToString()].Item2.Decimal18Column);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Decimal54Column, _checkValues[ChangeType.Delete.ToString()].Item1.Decimal54Column);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.FloatColumn, _checkValues[ChangeType.Delete.ToString()].Item1.FloatColumn);

            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);
        
            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<BigIntDecimalAndFloatTypesTestSqlServerModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.BigintColumn = e.Entity.BigintColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.Decimal18Column = e.Entity.Decimal18Column;
                    _checkValues[ChangeType.Insert.ToString()].Item2.Decimal54Column = e.Entity.Decimal54Column;
                    _checkValues[ChangeType.Insert.ToString()].Item2.FloatColumn = e.Entity.FloatColumn;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.BigintColumn = e.EntityOldValues.BigintColumn;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.Decimal18Column = e.EntityOldValues.Decimal18Column;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.Decimal54Column = e.EntityOldValues.Decimal54Column;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.FloatColumn = e.EntityOldValues.FloatColumn;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()] = null;
                    }

                    break;

                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.BigintColumn = e.Entity.BigintColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.Decimal18Column = e.Entity.Decimal18Column;
                    _checkValues[ChangeType.Update.ToString()].Item2.Decimal54Column = e.Entity.Decimal54Column;
                    _checkValues[ChangeType.Update.ToString()].Item2.FloatColumn = e.Entity.FloatColumn;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.BigintColumn = e.EntityOldValues.BigintColumn;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.Decimal18Column = e.EntityOldValues.Decimal18Column;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.Decimal54Column = e.EntityOldValues.Decimal54Column;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.FloatColumn = e.EntityOldValues.FloatColumn;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Update.ToString()] = null;
                    }

                    break;

                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.BigintColumn = e.Entity.BigintColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.Decimal18Column = e.Entity.Decimal18Column;
                    _checkValues[ChangeType.Delete.ToString()].Item2.Decimal54Column = e.Entity.Decimal54Column;
                    _checkValues[ChangeType.Delete.ToString()].Item2.FloatColumn = e.Entity.FloatColumn;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.BigintColumn = e.EntityOldValues.BigintColumn;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.Decimal18Column = e.EntityOldValues.Decimal18Column;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.Decimal54Column = e.EntityOldValues.Decimal54Column;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.FloatColumn = e.EntityOldValues.FloatColumn;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Delete.ToString()] = null;
                    }

                    break; ;
            }
        }

        private static void ModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([BigintColumn], [Decimal18Column], [Decimal54Column], [FloatColumn]) VALUES ({_checkValues[ChangeType.Insert.ToString()].Item1.BigintColumn}, @decimal18Column, null, null)";
                    sqlCommand.Parameters.Add(new SqlParameter("@decimal18Column", SqlDbType.Decimal) { Precision = 18, Scale = 0, Value = _checkValues[ChangeType.Insert.ToString()].Item1.Decimal18Column });
                    sqlCommand.ExecuteNonQuery();                    
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [BigintColumn] = null, [Decimal18Column] = null, [Decimal54Column] = @decimal54Column, [FloatColumn] = @floatColumn";
                    sqlCommand.Parameters.Add(new SqlParameter("@decimal54Column", SqlDbType.Decimal) { Value = _checkValues[ChangeType.Update.ToString()].Item1.Decimal54Column });
                    sqlCommand.Parameters.Add(new SqlParameter("@floatColumn", SqlDbType.Float) { Value = _checkValues[ChangeType.Update.ToString()].Item1.FloatColumn });
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