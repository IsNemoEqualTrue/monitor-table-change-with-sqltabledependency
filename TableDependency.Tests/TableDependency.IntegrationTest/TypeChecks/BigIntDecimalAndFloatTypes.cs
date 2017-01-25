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
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest.TypeChecks
{
    public class BigIntDecimalAndFloatModel
    {
        public long? bigintColumn { get; set; }
        public decimal? decimal18Column { get; set; }
        public decimal? decimal54Column { get; set; }
        public float? floatColumn { get; set; }
    }

    [TestClass]
    public class BigIntDecimalAndFloatTypesTestSqlServer
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServerConnectionString"].ConnectionString;
        private static string TableName = "CheckDecimalAndFloat";
        private static readonly Dictionary<string, Tuple<BigIntDecimalAndFloatModel, BigIntDecimalAndFloatModel>> CheckValues = new Dictionary<string, Tuple<BigIntDecimalAndFloatModel, BigIntDecimalAndFloatModel>>();

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
        public void Test()
        {
            SqlTableDependency<BigIntDecimalAndFloatModel> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<BigIntDecimalAndFloatModel>(ConnectionString, TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
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

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.bigintColumn, CheckValues[ChangeType.Insert.ToString()].Item1.bigintColumn);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.decimal18Column, CheckValues[ChangeType.Insert.ToString()].Item1.decimal18Column);
            Assert.IsNull(CheckValues[ChangeType.Insert.ToString()].Item2.decimal54Column);
            Assert.IsNull(CheckValues[ChangeType.Insert.ToString()].Item2.floatColumn);

            Assert.IsNull(CheckValues[ChangeType.Update.ToString()].Item2.bigintColumn);
            Assert.IsNull(CheckValues[ChangeType.Update.ToString()].Item2.decimal18Column);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.decimal54Column, CheckValues[ChangeType.Update.ToString()].Item1.decimal54Column);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.floatColumn, CheckValues[ChangeType.Update.ToString()].Item1.floatColumn);

            Assert.IsNull(CheckValues[ChangeType.Delete.ToString()].Item2.bigintColumn);
            Assert.IsNull(CheckValues[ChangeType.Delete.ToString()].Item2.decimal18Column);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.decimal54Column, CheckValues[ChangeType.Delete.ToString()].Item1.decimal54Column);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.floatColumn, CheckValues[ChangeType.Delete.ToString()].Item1.floatColumn);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<BigIntDecimalAndFloatModel> e)
        {

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.bigintColumn = e.Entity.bigintColumn;
                    CheckValues[ChangeType.Insert.ToString()].Item2.decimal18Column = e.Entity.decimal18Column;
                    CheckValues[ChangeType.Insert.ToString()].Item2.decimal54Column = e.Entity.decimal54Column;
                    CheckValues[ChangeType.Insert.ToString()].Item2.floatColumn = e.Entity.floatColumn;

                    break;
                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.bigintColumn = e.Entity.bigintColumn;
                    CheckValues[ChangeType.Update.ToString()].Item2.decimal18Column = e.Entity.decimal18Column;
                    CheckValues[ChangeType.Update.ToString()].Item2.decimal54Column = e.Entity.decimal54Column;
                    CheckValues[ChangeType.Update.ToString()].Item2.floatColumn = e.Entity.floatColumn;
                    break;
                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.bigintColumn = e.Entity.bigintColumn;
                    CheckValues[ChangeType.Delete.ToString()].Item2.decimal18Column = e.Entity.decimal18Column;
                    CheckValues[ChangeType.Delete.ToString()].Item2.decimal54Column = e.Entity.decimal54Column;
                    CheckValues[ChangeType.Delete.ToString()].Item2.floatColumn = e.Entity.floatColumn;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<BigIntDecimalAndFloatModel, BigIntDecimalAndFloatModel>(new BigIntDecimalAndFloatModel { bigintColumn = 123, decimal18Column = 987654321, decimal54Column = null, floatColumn = null }, new BigIntDecimalAndFloatModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<BigIntDecimalAndFloatModel, BigIntDecimalAndFloatModel>(new BigIntDecimalAndFloatModel { bigintColumn = null, decimal18Column = null, decimal54Column = 6.77M, floatColumn = 7.55F }, new BigIntDecimalAndFloatModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<BigIntDecimalAndFloatModel, BigIntDecimalAndFloatModel>(new BigIntDecimalAndFloatModel { bigintColumn = null, decimal18Column = null, decimal54Column = 6.77M, floatColumn = 7.55F }, new BigIntDecimalAndFloatModel()));

            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([bigintColumn], [decimal18Column], [decimal54Column], [floatColumn]) VALUES ({CheckValues[ChangeType.Insert.ToString()].Item1.bigintColumn}, @decimal18Column, null, null)";
                    sqlCommand.Parameters.Add(new SqlParameter("@decimal18Column", SqlDbType.Decimal)
                    {
                        Precision = 18,
                        Scale = 0,
                        Value = CheckValues[ChangeType.Insert.ToString()].Item1.decimal18Column
                    });
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [bigintColumn] = null, [decimal18Column] = null, [decimal54Column] = @decimal54Column, [floatColumn] = @floatColumn";
                    sqlCommand.Parameters.Add(new SqlParameter("@decimal54Column", SqlDbType.Decimal) { Value = CheckValues[ChangeType.Update.ToString()].Item1.decimal54Column });
                    sqlCommand.Parameters.Add(new SqlParameter("@floatColumn", SqlDbType.Float) { Value = CheckValues[ChangeType.Update.ToString()].Item1.floatColumn });
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