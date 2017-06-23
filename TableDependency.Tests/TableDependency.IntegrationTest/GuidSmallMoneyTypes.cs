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
using TableDependency.IntegrationTest.Helpers.SqlServer;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    public class ModelGuidSmallMoneyTypes
    {
        public Guid uniqueidentifierColumn { get; set; }
        public Nullable<TimeSpan> time7Column { get; set; }
        public byte tinyintColumn { get; set; }
        public DateTime smalldatetimeColumn { get; set; }
        public short smallintColumn { get; set; }
        public Decimal moneyColumn { get; set; }
        public Decimal smallmoneyColumn { get; set; }
    }

    [TestClass]
    public class GuidSmallMoneyTypes
    {
        private static string _connectionString = ConfigurationManager.ConnectionStrings["SqlServer2008 Test_User"].ConnectionString;
        private static string TableName = "CheckGuidSmallMoneyTimeStampTypes";
        private static Dictionary<string, Tuple<ModelGuidSmallMoneyTypes, ModelGuidSmallMoneyTypes>> _checkValues = new Dictionary<string, Tuple<ModelGuidSmallMoneyTypes, ModelGuidSmallMoneyTypes>>();

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
                        "uniqueidentifierColumn uniqueidentifier, " +
                        "time7Column time(7) NULL, " + 
                        "tinyintColumn tinyint NULL, " +                        
                        "smalldatetimeColumn smalldatetime NULL, " +
                        "smallintColumn smallint NULL, " +
                        "moneyColumn money NULL," +
                        "smallmoneyColumn smallmoney NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
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

        [TestCategory("SqlServer")]
        [TestMethod]
        public void Test()
        {
            SqlTableDependency<ModelGuidSmallMoneyTypes> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<ModelGuidSmallMoneyTypes>(_connectionString, TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000 * 1000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.uniqueidentifierColumn, _checkValues[ChangeType.Insert.ToString()].Item1.uniqueidentifierColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.time7Column, _checkValues[ChangeType.Insert.ToString()].Item1.time7Column);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.tinyintColumn, _checkValues[ChangeType.Insert.ToString()].Item1.tinyintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.smalldatetimeColumn, _checkValues[ChangeType.Insert.ToString()].Item1.smalldatetimeColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.smallintColumn, _checkValues[ChangeType.Insert.ToString()].Item1.smallintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.moneyColumn, _checkValues[ChangeType.Insert.ToString()].Item1.moneyColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.smallmoneyColumn, _checkValues[ChangeType.Insert.ToString()].Item1.smallmoneyColumn);

            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.smallintColumn, _checkValues[ChangeType.Update.ToString()].Item1.smallintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.time7Column, _checkValues[ChangeType.Update.ToString()].Item1.time7Column);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.tinyintColumn, _checkValues[ChangeType.Update.ToString()].Item1.tinyintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.smalldatetimeColumn, _checkValues[ChangeType.Update.ToString()].Item1.smalldatetimeColumn);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.smallintColumn, _checkValues[ChangeType.Update.ToString()].Item1.smallintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.moneyColumn, _checkValues[ChangeType.Update.ToString()].Item1.moneyColumn);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.smallmoneyColumn, _checkValues[ChangeType.Update.ToString()].Item1.smallmoneyColumn);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.smallintColumn, _checkValues[ChangeType.Delete.ToString()].Item1.smallintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.time7Column, _checkValues[ChangeType.Delete.ToString()].Item1.time7Column);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.tinyintColumn, _checkValues[ChangeType.Delete.ToString()].Item1.tinyintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.smalldatetimeColumn, _checkValues[ChangeType.Delete.ToString()].Item1.smalldatetimeColumn);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.smallintColumn, _checkValues[ChangeType.Delete.ToString()].Item1.smallintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.moneyColumn, _checkValues[ChangeType.Delete.ToString()].Item1.moneyColumn);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.smallmoneyColumn, _checkValues[ChangeType.Delete.ToString()].Item1.smallmoneyColumn);

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(naming));
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<ModelGuidSmallMoneyTypes> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.uniqueidentifierColumn = e.Entity.uniqueidentifierColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.time7Column = e.Entity.time7Column;
                    _checkValues[ChangeType.Insert.ToString()].Item2.tinyintColumn = e.Entity.tinyintColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.smalldatetimeColumn = e.Entity.smalldatetimeColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.smallintColumn = e.Entity.smallintColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.moneyColumn = e.Entity.moneyColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.smallmoneyColumn = e.Entity.smallmoneyColumn;
                    break;

                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.uniqueidentifierColumn = e.Entity.uniqueidentifierColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.time7Column = e.Entity.time7Column;
                    _checkValues[ChangeType.Update.ToString()].Item2.tinyintColumn = e.Entity.tinyintColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.smalldatetimeColumn = e.Entity.smalldatetimeColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.smallintColumn = e.Entity.smallintColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.moneyColumn = e.Entity.moneyColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.smallmoneyColumn = e.Entity.smallmoneyColumn;
                    break;

                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.uniqueidentifierColumn = e.Entity.uniqueidentifierColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.time7Column = e.Entity.time7Column;
                    _checkValues[ChangeType.Delete.ToString()].Item2.tinyintColumn = e.Entity.tinyintColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.smalldatetimeColumn = e.Entity.smalldatetimeColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.smallintColumn = e.Entity.smallintColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.moneyColumn = e.Entity.moneyColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.smallmoneyColumn = e.Entity.smallmoneyColumn;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            // https://msdn.microsoft.com/en-us/library/bb675168%28v=vs.110%29.aspx
            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<ModelGuidSmallMoneyTypes, ModelGuidSmallMoneyTypes>(new ModelGuidSmallMoneyTypes { uniqueidentifierColumn = Guid.NewGuid(), time7Column = DateTime.Parse("23:59:59").TimeOfDay, tinyintColumn = 1, smalldatetimeColumn  = DateTime.Now.Date, smallintColumn = 1, moneyColumn = 123.77M, smallmoneyColumn = 2.3M }, new ModelGuidSmallMoneyTypes()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<ModelGuidSmallMoneyTypes, ModelGuidSmallMoneyTypes>(new ModelGuidSmallMoneyTypes { uniqueidentifierColumn = Guid.NewGuid(), time7Column = DateTime.Parse("13:59:59").TimeOfDay, tinyintColumn = 2, smalldatetimeColumn = DateTime.Now.Date.AddDays(1), smallintColumn = 1, moneyColumn = 23.77M, smallmoneyColumn = 1.3M }, new ModelGuidSmallMoneyTypes()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<ModelGuidSmallMoneyTypes, ModelGuidSmallMoneyTypes>(new ModelGuidSmallMoneyTypes { uniqueidentifierColumn = _checkValues[ChangeType.Update.ToString()].Item2.uniqueidentifierColumn, time7Column = DateTime.Parse("13:59:59").TimeOfDay, tinyintColumn = 2, smalldatetimeColumn = DateTime.Now.Date.AddDays(1), smallintColumn = 1, moneyColumn = 23.77M, smallmoneyColumn = 1.3M }, new ModelGuidSmallMoneyTypes()));

            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] (uniqueidentifierColumn, time7Column, tinyintColumn, smalldatetimeColumn, smallintColumn, moneyColumn, smallmoneyColumn) " +
                        "values (@uniqueidentifierColumn, @time7Column, @tinyintColumn, @smalldatetimeColumn, @smallintColumn, @moneyColumn, @smallmoneyColumn)";

                    sqlCommand.Parameters.Add(new SqlParameter("@uniqueidentifierColumn", SqlDbType.UniqueIdentifier) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.uniqueidentifierColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@time7Column", SqlDbType.Time) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.time7Column });
                    sqlCommand.Parameters.Add(new SqlParameter("@tinyintColumn", SqlDbType.TinyInt) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.tinyintColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@smalldatetimeColumn", SqlDbType.SmallDateTime) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.smalldatetimeColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@smallintColumn", SqlDbType.SmallInt) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.smallintColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@moneyColumn", SqlDbType.Money) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.moneyColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@smallmoneyColumn", SqlDbType.SmallMoney) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.smallmoneyColumn });

                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET uniqueidentifierColumn = @uniqueidentifierColumn, time7Column = @time7Column, tinyintColumn = @tinyintColumn, smalldatetimeColumn = @smalldatetimeColumn, smallintColumn = @smallintColumn, moneyColumn = @moneyColumn, smallmoneyColumn = @smallmoneyColumn";

                    sqlCommand.Parameters.Add(new SqlParameter("@uniqueidentifierColumn", SqlDbType.UniqueIdentifier) { Value = _checkValues[ChangeType.Update.ToString()].Item1.uniqueidentifierColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@time7Column", SqlDbType.Time) { Value = _checkValues[ChangeType.Update.ToString()].Item1.time7Column });
                    sqlCommand.Parameters.Add(new SqlParameter("@tinyintColumn", SqlDbType.TinyInt) { Value = _checkValues[ChangeType.Update.ToString()].Item1.tinyintColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@smalldatetimeColumn", SqlDbType.SmallDateTime) { Value = _checkValues[ChangeType.Update.ToString()].Item1.smalldatetimeColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@smallintColumn", SqlDbType.SmallInt) { Value = _checkValues[ChangeType.Update.ToString()].Item1.smallintColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@moneyColumn", SqlDbType.Money) { Value = _checkValues[ChangeType.Update.ToString()].Item1.moneyColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@smallmoneyColumn", SqlDbType.SmallMoney) { Value = _checkValues[ChangeType.Update.ToString()].Item1.smallmoneyColumn });

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