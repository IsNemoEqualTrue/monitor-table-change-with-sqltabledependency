using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;

namespace TableDependency.SqlClient.Test
{
    public class GuidSmallMoneyTypesModel
    {
        public Guid UniqueidentifierColumn { get; set; }
        public TimeSpan? Time7Column { get; set; }
        public byte TinyintColumn { get; set; }
        public DateTime SmalldatetimeColumn { get; set; }
        public short SmallintColumn { get; set; }
        public Decimal MoneyColumn { get; set; }
        public Decimal SmallmoneyColumn { get; set; }
    }

    [TestClass]
    public class GuidSmallMoneyTypesTest : Base.SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(GuidSmallMoneyTypesModel).Name;
        private static Dictionary<string, Tuple<GuidSmallMoneyTypesModel, GuidSmallMoneyTypesModel>> _checkValues = new Dictionary<string, Tuple<GuidSmallMoneyTypesModel, GuidSmallMoneyTypesModel>>();
        private static Dictionary<string, Tuple<GuidSmallMoneyTypesModel, GuidSmallMoneyTypesModel>> _checkValuesOld = new Dictionary<string, Tuple<GuidSmallMoneyTypesModel, GuidSmallMoneyTypesModel>>();

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

            // https://msdn.microsoft.com/en-us/library/bb675168%28v=vs.110%29.aspx
            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<GuidSmallMoneyTypesModel, GuidSmallMoneyTypesModel>(new GuidSmallMoneyTypesModel { UniqueidentifierColumn = Guid.NewGuid(), Time7Column = DateTime.Parse("23:59:59").TimeOfDay, TinyintColumn = 1, SmalldatetimeColumn = DateTime.Now.Date, SmallintColumn = 1, MoneyColumn = 123.77M, SmallmoneyColumn = 2.3M }, new GuidSmallMoneyTypesModel()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<GuidSmallMoneyTypesModel, GuidSmallMoneyTypesModel>(new GuidSmallMoneyTypesModel { UniqueidentifierColumn = Guid.NewGuid(), Time7Column = DateTime.Parse("13:59:59").TimeOfDay, TinyintColumn = 2, SmalldatetimeColumn = DateTime.Now.Date.AddDays(1), SmallintColumn = 1, MoneyColumn = 23.77M, SmallmoneyColumn = 1.3M }, new GuidSmallMoneyTypesModel()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<GuidSmallMoneyTypesModel, GuidSmallMoneyTypesModel>(new GuidSmallMoneyTypesModel { UniqueidentifierColumn = _checkValues[ChangeType.Update.ToString()].Item2.UniqueidentifierColumn, Time7Column = DateTime.Parse("13:59:59").TimeOfDay, TinyintColumn = 2, SmalldatetimeColumn = DateTime.Now.Date.AddDays(1), SmallintColumn = 1, MoneyColumn = 23.77M, SmallmoneyColumn = 1.3M }, new GuidSmallMoneyTypesModel()));

            _checkValuesOld.Add(ChangeType.Insert.ToString(), new Tuple<GuidSmallMoneyTypesModel, GuidSmallMoneyTypesModel>(new GuidSmallMoneyTypesModel { UniqueidentifierColumn = Guid.NewGuid(), Time7Column = DateTime.Parse("23:59:59").TimeOfDay, TinyintColumn = 1, SmalldatetimeColumn = DateTime.Now.Date, SmallintColumn = 1, MoneyColumn = 123.77M, SmallmoneyColumn = 2.3M }, new GuidSmallMoneyTypesModel()));
            _checkValuesOld.Add(ChangeType.Update.ToString(), new Tuple<GuidSmallMoneyTypesModel, GuidSmallMoneyTypesModel>(new GuidSmallMoneyTypesModel { UniqueidentifierColumn = Guid.NewGuid(), Time7Column = DateTime.Parse("13:59:59").TimeOfDay, TinyintColumn = 2, SmalldatetimeColumn = DateTime.Now.Date.AddDays(1), SmallintColumn = 1, MoneyColumn = 23.77M, SmallmoneyColumn = 1.3M }, new GuidSmallMoneyTypesModel()));
            _checkValuesOld.Add(ChangeType.Delete.ToString(), new Tuple<GuidSmallMoneyTypesModel, GuidSmallMoneyTypesModel>(new GuidSmallMoneyTypesModel { UniqueidentifierColumn = _checkValues[ChangeType.Update.ToString()].Item2.UniqueidentifierColumn, Time7Column = DateTime.Parse("13:59:59").TimeOfDay, TinyintColumn = 2, SmalldatetimeColumn = DateTime.Now.Date.AddDays(1), SmallintColumn = 1, MoneyColumn = 23.77M, SmallmoneyColumn = 1.3M }, new GuidSmallMoneyTypesModel()));
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
            SqlTableDependency<GuidSmallMoneyTypesModel> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<GuidSmallMoneyTypesModel>(ConnectionStringForTestUser, tableName: TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
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

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.UniqueidentifierColumn, _checkValues[ChangeType.Insert.ToString()].Item1.UniqueidentifierColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Time7Column, _checkValues[ChangeType.Insert.ToString()].Item1.Time7Column);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.TinyintColumn, _checkValues[ChangeType.Insert.ToString()].Item1.TinyintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.SmalldatetimeColumn, _checkValues[ChangeType.Insert.ToString()].Item1.SmalldatetimeColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.SmallintColumn, _checkValues[ChangeType.Insert.ToString()].Item1.SmallintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.MoneyColumn, _checkValues[ChangeType.Insert.ToString()].Item1.MoneyColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.SmallmoneyColumn, _checkValues[ChangeType.Insert.ToString()].Item1.SmallmoneyColumn);
            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.SmallintColumn, _checkValues[ChangeType.Update.ToString()].Item1.SmallintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Time7Column, _checkValues[ChangeType.Update.ToString()].Item1.Time7Column);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.TinyintColumn, _checkValues[ChangeType.Update.ToString()].Item1.TinyintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.SmalldatetimeColumn, _checkValues[ChangeType.Update.ToString()].Item1.SmalldatetimeColumn);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.SmallintColumn, _checkValues[ChangeType.Update.ToString()].Item1.SmallintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.MoneyColumn, _checkValues[ChangeType.Update.ToString()].Item1.MoneyColumn);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.SmallmoneyColumn, _checkValues[ChangeType.Update.ToString()].Item1.SmallmoneyColumn);
            Assert.IsNull(_checkValuesOld[ChangeType.Update.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.SmallintColumn, _checkValues[ChangeType.Delete.ToString()].Item1.SmallintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Time7Column, _checkValues[ChangeType.Delete.ToString()].Item1.Time7Column);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.TinyintColumn, _checkValues[ChangeType.Delete.ToString()].Item1.TinyintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.SmalldatetimeColumn, _checkValues[ChangeType.Delete.ToString()].Item1.SmalldatetimeColumn);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.SmallintColumn, _checkValues[ChangeType.Delete.ToString()].Item1.SmallintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.MoneyColumn, _checkValues[ChangeType.Delete.ToString()].Item1.MoneyColumn);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.SmallmoneyColumn, _checkValues[ChangeType.Delete.ToString()].Item1.SmallmoneyColumn);
            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TestWithOldValues()
        {
            SqlTableDependency<GuidSmallMoneyTypesModel> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<GuidSmallMoneyTypesModel>(ConnectionStringForTestUser, includeOldValues: true, tableName: TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
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

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.UniqueidentifierColumn, _checkValues[ChangeType.Insert.ToString()].Item1.UniqueidentifierColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Time7Column, _checkValues[ChangeType.Insert.ToString()].Item1.Time7Column);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.TinyintColumn, _checkValues[ChangeType.Insert.ToString()].Item1.TinyintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.SmalldatetimeColumn, _checkValues[ChangeType.Insert.ToString()].Item1.SmalldatetimeColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.SmallintColumn, _checkValues[ChangeType.Insert.ToString()].Item1.SmallintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.MoneyColumn, _checkValues[ChangeType.Insert.ToString()].Item1.MoneyColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.SmallmoneyColumn, _checkValues[ChangeType.Insert.ToString()].Item1.SmallmoneyColumn);
            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.SmallintColumn, _checkValues[ChangeType.Update.ToString()].Item1.SmallintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Time7Column, _checkValues[ChangeType.Update.ToString()].Item1.Time7Column);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.TinyintColumn, _checkValues[ChangeType.Update.ToString()].Item1.TinyintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.SmalldatetimeColumn, _checkValues[ChangeType.Update.ToString()].Item1.SmalldatetimeColumn);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.SmallintColumn, _checkValues[ChangeType.Update.ToString()].Item1.SmallintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.MoneyColumn, _checkValues[ChangeType.Update.ToString()].Item1.MoneyColumn);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.SmallmoneyColumn, _checkValues[ChangeType.Update.ToString()].Item1.SmallmoneyColumn);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.SmallintColumn, _checkValues[ChangeType.Insert.ToString()].Item2.SmallintColumn);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.Time7Column, _checkValues[ChangeType.Insert.ToString()].Item2.Time7Column);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.TinyintColumn, _checkValues[ChangeType.Insert.ToString()].Item2.TinyintColumn);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.SmalldatetimeColumn, _checkValues[ChangeType.Insert.ToString()].Item2.SmalldatetimeColumn);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.SmallintColumn, _checkValues[ChangeType.Insert.ToString()].Item2.SmallintColumn);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.MoneyColumn, _checkValues[ChangeType.Insert.ToString()].Item2.MoneyColumn);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.SmallmoneyColumn, _checkValues[ChangeType.Insert.ToString()].Item2.SmallmoneyColumn);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.SmallintColumn, _checkValues[ChangeType.Delete.ToString()].Item1.SmallintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Time7Column, _checkValues[ChangeType.Delete.ToString()].Item1.Time7Column);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.TinyintColumn, _checkValues[ChangeType.Delete.ToString()].Item1.TinyintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.SmalldatetimeColumn, _checkValues[ChangeType.Delete.ToString()].Item1.SmalldatetimeColumn);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.SmallintColumn, _checkValues[ChangeType.Delete.ToString()].Item1.SmallintColumn);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.MoneyColumn, _checkValues[ChangeType.Delete.ToString()].Item1.MoneyColumn);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.SmallmoneyColumn, _checkValues[ChangeType.Delete.ToString()].Item1.SmallmoneyColumn);
            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<GuidSmallMoneyTypesModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.UniqueidentifierColumn = e.Entity.UniqueidentifierColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.Time7Column = e.Entity.Time7Column;
                    _checkValues[ChangeType.Insert.ToString()].Item2.TinyintColumn = e.Entity.TinyintColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.SmalldatetimeColumn = e.Entity.SmalldatetimeColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.SmallintColumn = e.Entity.SmallintColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.MoneyColumn = e.Entity.MoneyColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.SmallmoneyColumn = e.Entity.SmallmoneyColumn;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.UniqueidentifierColumn = e.EntityOldValues.UniqueidentifierColumn;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.Time7Column = e.EntityOldValues.Time7Column;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.TinyintColumn = e.EntityOldValues.TinyintColumn;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.SmalldatetimeColumn = e.EntityOldValues.SmalldatetimeColumn;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.SmallintColumn = e.EntityOldValues.SmallintColumn;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.MoneyColumn = e.EntityOldValues.MoneyColumn;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.SmallmoneyColumn = e.EntityOldValues.SmallmoneyColumn;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()] = null;
                    }

                    break;

                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.UniqueidentifierColumn = e.Entity.UniqueidentifierColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.Time7Column = e.Entity.Time7Column;
                    _checkValues[ChangeType.Update.ToString()].Item2.TinyintColumn = e.Entity.TinyintColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.SmalldatetimeColumn = e.Entity.SmalldatetimeColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.SmallintColumn = e.Entity.SmallintColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.MoneyColumn = e.Entity.MoneyColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.SmallmoneyColumn = e.Entity.SmallmoneyColumn;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.UniqueidentifierColumn = e.EntityOldValues.UniqueidentifierColumn;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.Time7Column = e.EntityOldValues.Time7Column;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.TinyintColumn = e.EntityOldValues.TinyintColumn;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.SmalldatetimeColumn = e.EntityOldValues.SmalldatetimeColumn;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.SmallintColumn = e.EntityOldValues.SmallintColumn;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.MoneyColumn = e.EntityOldValues.MoneyColumn;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.SmallmoneyColumn = e.EntityOldValues.SmallmoneyColumn;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Update.ToString()] = null;
                    }

                    break;

                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.UniqueidentifierColumn = e.Entity.UniqueidentifierColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.Time7Column = e.Entity.Time7Column;
                    _checkValues[ChangeType.Delete.ToString()].Item2.TinyintColumn = e.Entity.TinyintColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.SmalldatetimeColumn = e.Entity.SmalldatetimeColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.SmallintColumn = e.Entity.SmallintColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.MoneyColumn = e.Entity.MoneyColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.SmallmoneyColumn = e.Entity.SmallmoneyColumn;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.UniqueidentifierColumn = e.EntityOldValues.UniqueidentifierColumn;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.Time7Column = e.EntityOldValues.Time7Column;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.TinyintColumn = e.EntityOldValues.TinyintColumn;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.SmalldatetimeColumn = e.EntityOldValues.SmalldatetimeColumn;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.SmallintColumn = e.EntityOldValues.SmallintColumn;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.MoneyColumn = e.EntityOldValues.MoneyColumn;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.SmallmoneyColumn = e.EntityOldValues.SmallmoneyColumn;
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
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] (uniqueidentifierColumn, time7Column, tinyintColumn, smalldatetimeColumn, smallintColumn, moneyColumn, smallmoneyColumn) " +
                        "values (@uniqueidentifierColumn, @time7Column, @tinyintColumn, @smalldatetimeColumn, @smallintColumn, @moneyColumn, @smallmoneyColumn)";

                    sqlCommand.Parameters.Add(new SqlParameter("@uniqueidentifierColumn", SqlDbType.UniqueIdentifier) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.UniqueidentifierColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@time7Column", SqlDbType.Time) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.Time7Column });
                    sqlCommand.Parameters.Add(new SqlParameter("@tinyintColumn", SqlDbType.TinyInt) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.TinyintColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@smalldatetimeColumn", SqlDbType.SmallDateTime) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.SmalldatetimeColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@smallintColumn", SqlDbType.SmallInt) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.SmallintColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@moneyColumn", SqlDbType.Money) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.MoneyColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@smallmoneyColumn", SqlDbType.SmallMoney) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.SmallmoneyColumn });

                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET uniqueidentifierColumn = @uniqueidentifierColumn, time7Column = @time7Column, tinyintColumn = @tinyintColumn, smalldatetimeColumn = @smalldatetimeColumn, smallintColumn = @smallintColumn, moneyColumn = @moneyColumn, smallmoneyColumn = @smallmoneyColumn";

                    sqlCommand.Parameters.Add(new SqlParameter("@uniqueidentifierColumn", SqlDbType.UniqueIdentifier) { Value = _checkValues[ChangeType.Update.ToString()].Item1.UniqueidentifierColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@time7Column", SqlDbType.Time) { Value = _checkValues[ChangeType.Update.ToString()].Item1.Time7Column });
                    sqlCommand.Parameters.Add(new SqlParameter("@tinyintColumn", SqlDbType.TinyInt) { Value = _checkValues[ChangeType.Update.ToString()].Item1.TinyintColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@smalldatetimeColumn", SqlDbType.SmallDateTime) { Value = _checkValues[ChangeType.Update.ToString()].Item1.SmalldatetimeColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@smallintColumn", SqlDbType.SmallInt) { Value = _checkValues[ChangeType.Update.ToString()].Item1.SmallintColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@moneyColumn", SqlDbType.Money) { Value = _checkValues[ChangeType.Update.ToString()].Item1.MoneyColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@smallmoneyColumn", SqlDbType.SmallMoney) { Value = _checkValues[ChangeType.Update.ToString()].Item1.SmallmoneyColumn });

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