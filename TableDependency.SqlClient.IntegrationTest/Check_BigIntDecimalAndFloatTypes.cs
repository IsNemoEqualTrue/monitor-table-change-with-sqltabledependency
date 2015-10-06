using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient.IntegrationTest.Helpers;
using TableDependency.SqlClient.IntegrationTest.Model;

namespace TableDependency.SqlClient.IntegrationTest
{
    [TestClass]
    public class Check_BigIntDecimalAndFloatTypes
    {
        private static string _connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private static string TableName = "CheckDecimalAndFloat";
        private static Dictionary<string, Tuple<Check_Model, Check_Model>> _checkValues = new Dictionary<string, Tuple<Check_Model, Check_Model>>();

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

                    sqlCommand.CommandText = $"CREATE TABLE {TableName}(" +
                        "bigintColumn BIGINT NULL," +
                        "decimal18Column decimal(18, 0) NULL, " +
                        "decimal54Column decimal(5, 4) NULL, " +
                        "floatColumn float NULL)";
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

        [TestMethod]
        public void Test()
        {
            SqlTableDependency<Check_Model> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<Check_Model>(_connectionString, TableName);
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

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.bigintColumn, _checkValues[ChangeType.Insert.ToString()].Item1.bigintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.decimal18Column, _checkValues[ChangeType.Insert.ToString()].Item1.decimal18Column);
            Assert.IsNull(_checkValues[ChangeType.Insert.ToString()].Item2.decimal54Column);
            Assert.IsNull(_checkValues[ChangeType.Insert.ToString()].Item2.floatColumn);

            Assert.IsNull(_checkValues[ChangeType.Update.ToString()].Item2.bigintColumn);
            Assert.IsNull(_checkValues[ChangeType.Update.ToString()].Item2.decimal18Column);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.decimal54Column, _checkValues[ChangeType.Update.ToString()].Item1.decimal54Column);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.floatColumn, _checkValues[ChangeType.Update.ToString()].Item1.floatColumn);

            Assert.IsNull(_checkValues[ChangeType.Delete.ToString()].Item2.bigintColumn);
            Assert.IsNull(_checkValues[ChangeType.Delete.ToString()].Item2.decimal18Column);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.decimal54Column, _checkValues[ChangeType.Delete.ToString()].Item1.decimal54Column);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.floatColumn, _checkValues[ChangeType.Delete.ToString()].Item1.floatColumn);

            Assert.IsTrue(Helper.AreAllDbObjectDisposed(_connectionString, naming));
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<Check_Model> e)
        {

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.bigintColumn = e.Entity.bigintColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.decimal18Column = e.Entity.decimal18Column;
                    _checkValues[ChangeType.Insert.ToString()].Item2.decimal54Column = e.Entity.decimal54Column;
                    _checkValues[ChangeType.Insert.ToString()].Item2.floatColumn = e.Entity.floatColumn;

                    break;
                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.bigintColumn = e.Entity.bigintColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.decimal18Column = e.Entity.decimal18Column;
                    _checkValues[ChangeType.Update.ToString()].Item2.decimal54Column = e.Entity.decimal54Column;
                    _checkValues[ChangeType.Update.ToString()].Item2.floatColumn = e.Entity.floatColumn;
                    break;
                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.bigintColumn = e.Entity.bigintColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.decimal18Column = e.Entity.decimal18Column;
                    _checkValues[ChangeType.Delete.ToString()].Item2.decimal54Column = e.Entity.decimal54Column;
                    _checkValues[ChangeType.Delete.ToString()].Item2.floatColumn = e.Entity.floatColumn;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<Check_Model, Check_Model>(new Check_Model { bigintColumn = 123, decimal18Column = 987654321, decimal54Column = null, floatColumn = null }, new Check_Model()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<Check_Model, Check_Model>(new Check_Model { bigintColumn = null, decimal18Column = null, decimal54Column = 6.77M, floatColumn = 7.55F }, new Check_Model()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<Check_Model, Check_Model>(new Check_Model { bigintColumn = null, decimal18Column = null, decimal54Column = 6.77M, floatColumn = 7.55F }, new Check_Model()));

            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([bigintColumn], [decimal18Column], [decimal54Column], [floatColumn]) VALUES ({_checkValues[ChangeType.Insert.ToString()].Item1.bigintColumn}, @decimal18Column, null, null)";
                    sqlCommand.Parameters.Add(new SqlParameter("@decimal18Column", SqlDbType.Decimal)
                    {
                        Precision = 18,
                        Scale = 0,
                        Value = _checkValues[ChangeType.Insert.ToString()].Item1.decimal18Column
                    });
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [bigintColumn] = null, [decimal18Column] = null, [decimal54Column] = @decimal54Column, [floatColumn] = @floatColumn";
                    sqlCommand.Parameters.Add(new SqlParameter("@decimal54Column", SqlDbType.Decimal) { Value = _checkValues[ChangeType.Update.ToString()].Item1.decimal54Column });
                    sqlCommand.Parameters.Add(new SqlParameter("@floatColumn", SqlDbType.Float) { Value = _checkValues[ChangeType.Update.ToString()].Item1.floatColumn });
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);
                }
            }
        }
    }
}