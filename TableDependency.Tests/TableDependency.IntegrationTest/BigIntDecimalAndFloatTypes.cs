using System;
using System.Collections.Generic;
using System.Data;
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
    public class BigIntDecimalAndFloatTypesTestSqlServerModel
    {
        public long? BigintColumn { get; set; }
        public decimal? Decimal18Column { get; set; }
        public decimal? Decimal54Column { get; set; }
        public float? FloatColumn { get; set; }
    }

    [TestClass]
    public class BigIntDecimalAndFloatTypesTestSqlServer : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(BigIntDecimalAndFloatTypesTestSqlServerModel).Name;
        private static readonly Dictionary<string, Tuple<BigIntDecimalAndFloatTypesTestSqlServerModel, BigIntDecimalAndFloatTypesTestSqlServerModel>> CheckValues = new Dictionary<string, Tuple<BigIntDecimalAndFloatTypesTestSqlServerModel, BigIntDecimalAndFloatTypesTestSqlServerModel>>();

        [ClassInitialize()]
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

        [TestInitialize()]
        public void TestInitialize()
        {
        }

        [ClassCleanup()]
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
                tableDependency = new SqlTableDependency<BigIntDecimalAndFloatTypesTestSqlServerModel>(ConnectionStringForTestUser, TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 10 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.BigintColumn, CheckValues[ChangeType.Insert.ToString()].Item1.BigintColumn);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Decimal18Column, CheckValues[ChangeType.Insert.ToString()].Item1.Decimal18Column);
            Assert.IsNull(CheckValues[ChangeType.Insert.ToString()].Item2.Decimal54Column);
            Assert.IsNull(CheckValues[ChangeType.Insert.ToString()].Item2.FloatColumn);

            Assert.IsNull(CheckValues[ChangeType.Update.ToString()].Item2.BigintColumn);
            Assert.IsNull(CheckValues[ChangeType.Update.ToString()].Item2.Decimal18Column);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Decimal54Column, CheckValues[ChangeType.Update.ToString()].Item1.Decimal54Column);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.FloatColumn, CheckValues[ChangeType.Update.ToString()].Item1.FloatColumn);

            Assert.IsNull(CheckValues[ChangeType.Delete.ToString()].Item2.BigintColumn);
            Assert.IsNull(CheckValues[ChangeType.Delete.ToString()].Item2.Decimal18Column);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Decimal54Column, CheckValues[ChangeType.Delete.ToString()].Item1.Decimal54Column);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.FloatColumn, CheckValues[ChangeType.Delete.ToString()].Item1.FloatColumn);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<BigIntDecimalAndFloatTypesTestSqlServerModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.BigintColumn = e.Entity.BigintColumn;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Decimal18Column = e.Entity.Decimal18Column;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Decimal54Column = e.Entity.Decimal54Column;
                    CheckValues[ChangeType.Insert.ToString()].Item2.FloatColumn = e.Entity.FloatColumn;
                    break;

                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.BigintColumn = e.Entity.BigintColumn;
                    CheckValues[ChangeType.Update.ToString()].Item2.Decimal18Column = e.Entity.Decimal18Column;
                    CheckValues[ChangeType.Update.ToString()].Item2.Decimal54Column = e.Entity.Decimal54Column;
                    CheckValues[ChangeType.Update.ToString()].Item2.FloatColumn = e.Entity.FloatColumn;
                    break;

                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.BigintColumn = e.Entity.BigintColumn;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Decimal18Column = e.Entity.Decimal18Column;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Decimal54Column = e.Entity.Decimal54Column;
                    CheckValues[ChangeType.Delete.ToString()].Item2.FloatColumn = e.Entity.FloatColumn;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<BigIntDecimalAndFloatTypesTestSqlServerModel, BigIntDecimalAndFloatTypesTestSqlServerModel>(new BigIntDecimalAndFloatTypesTestSqlServerModel { BigintColumn = 123, Decimal18Column = 987654321, Decimal54Column = null, FloatColumn = null }, new BigIntDecimalAndFloatTypesTestSqlServerModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<BigIntDecimalAndFloatTypesTestSqlServerModel, BigIntDecimalAndFloatTypesTestSqlServerModel>(new BigIntDecimalAndFloatTypesTestSqlServerModel { BigintColumn = null, Decimal18Column = null, Decimal54Column = 6.77M, FloatColumn = 7.55F }, new BigIntDecimalAndFloatTypesTestSqlServerModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<BigIntDecimalAndFloatTypesTestSqlServerModel, BigIntDecimalAndFloatTypesTestSqlServerModel>(new BigIntDecimalAndFloatTypesTestSqlServerModel { BigintColumn = null, Decimal18Column = null, Decimal54Column = 6.77M, FloatColumn = 7.55F }, new BigIntDecimalAndFloatTypesTestSqlServerModel()));

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([BigintColumn], [Decimal18Column], [Decimal54Column], [FloatColumn]) VALUES ({CheckValues[ChangeType.Insert.ToString()].Item1.BigintColumn}, @decimal18Column, null, null)";
                    sqlCommand.Parameters.Add(new SqlParameter("@decimal18Column", SqlDbType.Decimal) { Precision = 18, Scale = 0, Value = CheckValues[ChangeType.Insert.ToString()].Item1.Decimal18Column });
                    sqlCommand.ExecuteNonQuery();                    
                }

                Thread.Sleep(5000);

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [BigintColumn] = null, [Decimal18Column] = null, [Decimal54Column] = @decimal54Column, [FloatColumn] = @floatColumn";
                    sqlCommand.Parameters.Add(new SqlParameter("@decimal54Column", SqlDbType.Decimal) { Value = CheckValues[ChangeType.Update.ToString()].Item1.Decimal54Column });
                    sqlCommand.Parameters.Add(new SqlParameter("@floatColumn", SqlDbType.Float) { Value = CheckValues[ChangeType.Update.ToString()].Item1.FloatColumn });
                    sqlCommand.ExecuteNonQuery();                    
                }

                Thread.Sleep(5000);

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();                    
                }
            }
        }
    }
}