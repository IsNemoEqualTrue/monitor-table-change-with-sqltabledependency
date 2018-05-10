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
    public class DateTypeTestModel
    {
        public DateTime? DateColumn { get; set; }
        public DateTime? DatetimeColumn { get; set; }
        public DateTime? Datetime2Column { get; set; }
        public DateTimeOffset? DatetimeoffsetColumn { get; set; }
    }

    [TestClass]
    public class DateTypeTest : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(DateTypeTestModel).Name;
        private static Dictionary<string, Tuple<DateTypeTestModel, DateTypeTestModel>> _checkValues = new Dictionary<string, Tuple<DateTypeTestModel, DateTypeTestModel>>();
        private static Dictionary<string, Tuple<DateTypeTestModel, DateTypeTestModel>> _checkValuesOld = new Dictionary<string, Tuple<DateTypeTestModel, DateTypeTestModel>>();

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

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}] (" +
                        "dateColumn date NULL, " +
                        "datetimeColumn DATETIME NULL, " +
                        "datetime2Column datetime2(7) NULL, " +
                        "datetimeoffsetColumn DATETIMEOFFSET(7) NULL)";
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

            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<DateTypeTestModel, DateTypeTestModel>(new DateTypeTestModel { DateColumn = DateTime.Now.AddDays(-1).Date, DatetimeColumn = null, Datetime2Column = DateTime.Now.AddDays(-3), DatetimeoffsetColumn = DateTimeOffset.Now.AddDays(-4) }, new DateTypeTestModel()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<DateTypeTestModel, DateTypeTestModel>(new DateTypeTestModel { DateColumn = null, DatetimeColumn = DateTime.Now, Datetime2Column = null, DatetimeoffsetColumn = DateTime.Now }, new DateTypeTestModel()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<DateTypeTestModel, DateTypeTestModel>(new DateTypeTestModel { DateColumn = null, DatetimeColumn = DateTime.Now, Datetime2Column = null, DatetimeoffsetColumn = DateTime.Now }, new DateTypeTestModel()));

            _checkValuesOld.Add(ChangeType.Insert.ToString(), new Tuple<DateTypeTestModel, DateTypeTestModel>(new DateTypeTestModel { DateColumn = DateTime.Now.AddDays(-1).Date, DatetimeColumn = null, Datetime2Column = DateTime.Now.AddDays(-3), DatetimeoffsetColumn = DateTimeOffset.Now.AddDays(-4) }, new DateTypeTestModel()));
            _checkValuesOld.Add(ChangeType.Update.ToString(), new Tuple<DateTypeTestModel, DateTypeTestModel>(new DateTypeTestModel { DateColumn = null, DatetimeColumn = DateTime.Now, Datetime2Column = null, DatetimeoffsetColumn = DateTime.Now }, new DateTypeTestModel()));
            _checkValuesOld.Add(ChangeType.Delete.ToString(), new Tuple<DateTypeTestModel, DateTypeTestModel>(new DateTypeTestModel { DateColumn = null, DatetimeColumn = DateTime.Now, Datetime2Column = null, DatetimeoffsetColumn = DateTime.Now }, new DateTypeTestModel()));
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
        public void Test()
        {
            SqlTableDependency<DateTypeTestModel> tableDependency = null;

            try
            {
                tableDependency = new SqlTableDependency<DateTypeTestModel>(ConnectionStringForTestUser);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.Start();             

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 5 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.DateColumn, _checkValues[ChangeType.Insert.ToString()].Item1.DateColumn);
            Assert.IsNull(_checkValues[ChangeType.Insert.ToString()].Item2.DatetimeColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Datetime2Column, _checkValues[ChangeType.Insert.ToString()].Item1.Datetime2Column);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.DatetimeoffsetColumn, _checkValues[ChangeType.Insert.ToString()].Item1.DatetimeoffsetColumn);
            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.IsNull(_checkValues[ChangeType.Update.ToString()].Item2.DateColumn);
            var date1 = _checkValues[ChangeType.Update.ToString()].Item1.DatetimeColumn.GetValueOrDefault().AddMilliseconds(-_checkValues[ChangeType.Update.ToString()].Item1.DatetimeColumn.GetValueOrDefault().Millisecond);
            var date2 = _checkValues[ChangeType.Update.ToString()].Item2.DatetimeColumn.GetValueOrDefault().AddMilliseconds(-_checkValues[ChangeType.Update.ToString()].Item2.DatetimeColumn.GetValueOrDefault().Millisecond);
            Assert.AreEqual(date1.ToString("yyyyMMddhhmm"), date2.ToString("yyyyMMddhhmm"));
            Assert.IsNull(_checkValues[ChangeType.Update.ToString()].Item2.Datetime2Column);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.DatetimeoffsetColumn, _checkValues[ChangeType.Update.ToString()].Item1.DatetimeoffsetColumn);
            Assert.IsNull(_checkValuesOld[ChangeType.Update.ToString()]);

            Assert.IsNull(_checkValues[ChangeType.Delete.ToString()].Item2.DateColumn);
            date1 = _checkValues[ChangeType.Update.ToString()].Item1.DatetimeColumn.GetValueOrDefault().AddMilliseconds(-_checkValues[ChangeType.Update.ToString()].Item1.DatetimeColumn.GetValueOrDefault().Millisecond);
            date2 = _checkValues[ChangeType.Update.ToString()].Item2.DatetimeColumn.GetValueOrDefault().AddMilliseconds(-_checkValues[ChangeType.Update.ToString()].Item2.DatetimeColumn.GetValueOrDefault().Millisecond);
            Assert.AreEqual(date1.ToString("yyyyMMddhhmm"), date2.ToString("yyyyMMddhhmm"));
            Assert.IsNull(_checkValues[ChangeType.Delete.ToString()].Item2.Datetime2Column);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.DatetimeoffsetColumn.GetValueOrDefault().ToString("yyyyMMddhhmm"), _checkValues[ChangeType.Delete.ToString()].Item1.DatetimeoffsetColumn.GetValueOrDefault().ToString("yyyyMMddhhmm"));
            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(tableDependency.DataBaseObjectsNamingConvention));
            Assert.IsTrue(base.CountConversationEndpoints(tableDependency.DataBaseObjectsNamingConvention) == 0);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<DateTypeTestModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.DateColumn = e.Entity.DateColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.DatetimeColumn = e.Entity.DatetimeColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.Datetime2Column = e.Entity.Datetime2Column;
                    _checkValues[ChangeType.Insert.ToString()].Item2.DatetimeoffsetColumn = e.Entity.DatetimeoffsetColumn;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.DateColumn = e.EntityOldValues.DateColumn;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.DatetimeColumn = e.EntityOldValues.DatetimeColumn;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.Datetime2Column = e.EntityOldValues.Datetime2Column;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.DatetimeoffsetColumn = e.EntityOldValues.DatetimeoffsetColumn;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()] = null;
                    }

                    break;

                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.DateColumn = e.Entity.DateColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.DatetimeColumn = e.Entity.DatetimeColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.Datetime2Column = e.Entity.Datetime2Column;
                    _checkValues[ChangeType.Update.ToString()].Item2.DatetimeoffsetColumn = e.Entity.DatetimeoffsetColumn;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.DateColumn = e.EntityOldValues.DateColumn;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.DatetimeColumn = e.EntityOldValues.DatetimeColumn;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.Datetime2Column = e.EntityOldValues.Datetime2Column;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.DatetimeoffsetColumn = e.EntityOldValues.DatetimeoffsetColumn;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Update.ToString()] = null;
                    }

                    break;

                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.DateColumn = e.Entity.DateColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.DatetimeColumn = e.Entity.DatetimeColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.Datetime2Column = e.Entity.Datetime2Column;
                    _checkValues[ChangeType.Delete.ToString()].Item2.DatetimeoffsetColumn = e.Entity.DatetimeoffsetColumn;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.DateColumn = e.EntityOldValues.DateColumn;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.DatetimeColumn = e.EntityOldValues.DatetimeColumn;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.Datetime2Column = e.EntityOldValues.Datetime2Column;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.DatetimeoffsetColumn = e.EntityOldValues.DatetimeoffsetColumn;
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
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([dateColumn], [datetimeColumn], [datetime2Column], [datetimeoffsetColumn]) VALUES(@dateColumn, NULL, @datetime2Column, @datetimeoffsetColumn)";
                    sqlCommand.Parameters.Add(new SqlParameter("@dateColumn", SqlDbType.Date) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.DateColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@datetime2Column", SqlDbType.DateTime2) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.Datetime2Column });
                    sqlCommand.Parameters.Add(new SqlParameter("@datetimeoffsetColumn", SqlDbType.DateTimeOffset) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.DatetimeoffsetColumn });
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [dateColumn] = NULL, [datetimeColumn] = @datetimeColumn, [datetime2Column] = NULL, [datetimeoffsetColumn] = @datetimeoffsetColumn";
                    sqlCommand.Parameters.Add(new SqlParameter("@datetimeColumn", SqlDbType.DateTime) { Value = _checkValues[ChangeType.Update.ToString()].Item1.DatetimeColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@datetimeoffsetColumn", SqlDbType.DateTimeOffset) { Value = _checkValues[ChangeType.Update.ToString()].Item1.DatetimeoffsetColumn });
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