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
    public class Check_DateType
    {
        private static string _connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private static string TableName = "Test";
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

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}] (" +
                        "dateColumn date NULL, " +
                        "datetimeColumn datetime NULL, " +
                        "datetime2Column datetime2(7) NULL, " +
                        "datetimeoffsetColumn datetimeoffset(7) NULL)";
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
        public void CheckDateTypeTest()
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

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.dateColumn, _checkValues[ChangeType.Insert.ToString()].Item1.dateColumn);
            Assert.IsNull(_checkValues[ChangeType.Insert.ToString()].Item2.datetimeColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.datetime2Column, _checkValues[ChangeType.Insert.ToString()].Item1.datetime2Column);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.datetimeoffsetColumn, _checkValues[ChangeType.Insert.ToString()].Item1.datetimeoffsetColumn);

            Assert.IsNull(_checkValues[ChangeType.Update.ToString()].Item2.dateColumn);
            var date1 = _checkValues[ChangeType.Update.ToString()].Item1.datetimeColumn.GetValueOrDefault().AddMilliseconds(-_checkValues[ChangeType.Update.ToString()].Item1.datetimeColumn.GetValueOrDefault().Millisecond);
            var date2 = _checkValues[ChangeType.Update.ToString()].Item2.datetimeColumn.GetValueOrDefault().AddMilliseconds(-_checkValues[ChangeType.Update.ToString()].Item2.datetimeColumn.GetValueOrDefault().Millisecond);
            Assert.AreEqual(date1.ToString("yyyyMMddhhmm"), date2.ToString("yyyyMMddhhmm"));
            Assert.IsNull(_checkValues[ChangeType.Update.ToString()].Item2.datetime2Column);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.datetimeoffsetColumn, _checkValues[ChangeType.Update.ToString()].Item1.datetimeoffsetColumn);

            Assert.IsNull(_checkValues[ChangeType.Delete.ToString()].Item2.dateColumn);
            date1 = _checkValues[ChangeType.Update.ToString()].Item1.datetimeColumn.GetValueOrDefault().AddMilliseconds(-_checkValues[ChangeType.Update.ToString()].Item1.datetimeColumn.GetValueOrDefault().Millisecond);
            date2 = _checkValues[ChangeType.Update.ToString()].Item2.datetimeColumn.GetValueOrDefault().AddMilliseconds(-_checkValues[ChangeType.Update.ToString()].Item2.datetimeColumn.GetValueOrDefault().Millisecond);
            Assert.AreEqual(date1.ToString("yyyyMMddhhmm"), date2.ToString("yyyyMMddhhmm")); Assert.IsNull(_checkValues[ChangeType.Delete.ToString()].Item2.datetime2Column);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.datetimeoffsetColumn, _checkValues[ChangeType.Delete.ToString()].Item1.datetimeoffsetColumn);

            Assert.IsTrue(Helper.AreAllDbObjectDisposed(_connectionString, naming));
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<Check_Model> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.dateColumn = e.Entity.dateColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.datetimeColumn = e.Entity.datetimeColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.datetime2Column = e.Entity.datetime2Column;
                    _checkValues[ChangeType.Insert.ToString()].Item2.datetimeoffsetColumn = e.Entity.datetimeoffsetColumn;
                    break;
                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.dateColumn = e.Entity.dateColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.datetimeColumn = e.Entity.datetimeColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.datetime2Column = e.Entity.datetime2Column;
                    _checkValues[ChangeType.Update.ToString()].Item2.datetimeoffsetColumn = e.Entity.datetimeoffsetColumn;
                    break;
                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.dateColumn = e.Entity.dateColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.datetimeColumn = e.Entity.datetimeColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.datetime2Column = e.Entity.datetime2Column;
                    _checkValues[ChangeType.Delete.ToString()].Item2.datetimeoffsetColumn = e.Entity.datetimeoffsetColumn;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<Check_Model, Check_Model>(new Check_Model { dateColumn =  DateTime.Now.AddDays(-1).Date, datetimeColumn = null, datetime2Column = DateTime.Now.AddDays(-3), datetimeoffsetColumn = DateTimeOffset.Now.AddDays(-4) }, new Check_Model()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<Check_Model, Check_Model>(new Check_Model { dateColumn = null, datetimeColumn = DateTime.Now, datetime2Column = null, datetimeoffsetColumn = DateTime.Now }, new Check_Model()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<Check_Model, Check_Model>(new Check_Model { dateColumn = null, datetimeColumn = DateTime.Now, datetime2Column = null, datetimeoffsetColumn = DateTime.Now }, new Check_Model()));

            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([dateColumn], [datetimeColumn], [datetime2Column], [datetimeoffsetColumn]) VALUES(@dateColumn, NULL, @datetime2Column, @datetimeoffsetColumn)";
                    sqlCommand.Parameters.Add(new SqlParameter("@dateColumn", SqlDbType.Date) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.dateColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@datetime2Column", SqlDbType.DateTime2) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.datetime2Column });
                    sqlCommand.Parameters.Add(new SqlParameter("@datetimeoffsetColumn", SqlDbType.DateTimeOffset) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.datetimeoffsetColumn });
                    sqlCommand.ExecuteNonQuery();
                }

                Thread.Sleep(1000);

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [dateColumn] = NULL, [datetimeColumn] = @datetimeColumn, [datetime2Column] = NULL, [datetimeoffsetColumn] = @datetimeoffsetColumn";
                    sqlCommand.Parameters.Add(new SqlParameter("@datetimeColumn", SqlDbType.DateTime) { Value = _checkValues[ChangeType.Update.ToString()].Item1.datetimeColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@datetimeoffsetColumn", SqlDbType.DateTimeOffset) { Value = _checkValues[ChangeType.Update.ToString()].Item1.datetimeoffsetColumn });
                    sqlCommand.ExecuteNonQuery();
                }

                Thread.Sleep(1000);

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }

                Thread.Sleep(1000);
            }
        }
    }
}