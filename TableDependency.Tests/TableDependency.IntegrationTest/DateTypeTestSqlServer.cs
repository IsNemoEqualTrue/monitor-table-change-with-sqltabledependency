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
    public class DateTypeTestModel
    {
        public DateTime? dateColumn { get; set; }
        public DateTime? datetimeColumn { get; set; }
        public DateTime? datetime2Column { get; set; }
        public DateTimeOffset? datetimeoffsetColumn { get; set; }
    }

    [TestClass]
    public class DateTypeTest
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServer2008 Test_User"].ConnectionString;
        private static string TableName = "Test";
        private static readonly Dictionary<string, Tuple<DateTypeTestModel, DateTypeTestModel>> CheckValues = new Dictionary<string, Tuple<DateTypeTestModel, DateTypeTestModel>>();

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
        public void CheckDateTypeTest()
        {
            SqlTableDependency<DateTypeTestModel> tableDependency = null;

            try
            {
                tableDependency = new SqlTableDependency<DateTypeTestModel>(ConnectionString, TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.Start();
              

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.dateColumn, CheckValues[ChangeType.Insert.ToString()].Item1.dateColumn);
            Assert.IsNull(CheckValues[ChangeType.Insert.ToString()].Item2.datetimeColumn);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.datetime2Column, CheckValues[ChangeType.Insert.ToString()].Item1.datetime2Column);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.datetimeoffsetColumn, CheckValues[ChangeType.Insert.ToString()].Item1.datetimeoffsetColumn);

            Assert.IsNull(CheckValues[ChangeType.Update.ToString()].Item2.dateColumn);
            var date1 = CheckValues[ChangeType.Update.ToString()].Item1.datetimeColumn.GetValueOrDefault().AddMilliseconds(-CheckValues[ChangeType.Update.ToString()].Item1.datetimeColumn.GetValueOrDefault().Millisecond);
            var date2 = CheckValues[ChangeType.Update.ToString()].Item2.datetimeColumn.GetValueOrDefault().AddMilliseconds(-CheckValues[ChangeType.Update.ToString()].Item2.datetimeColumn.GetValueOrDefault().Millisecond);
            Assert.AreEqual(date1.ToString("yyyyMMddhhmm"), date2.ToString("yyyyMMddhhmm"));
            Assert.IsNull(CheckValues[ChangeType.Update.ToString()].Item2.datetime2Column);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.datetimeoffsetColumn, CheckValues[ChangeType.Update.ToString()].Item1.datetimeoffsetColumn);

            Assert.IsNull(CheckValues[ChangeType.Delete.ToString()].Item2.dateColumn);
            date1 = CheckValues[ChangeType.Update.ToString()].Item1.datetimeColumn.GetValueOrDefault().AddMilliseconds(-CheckValues[ChangeType.Update.ToString()].Item1.datetimeColumn.GetValueOrDefault().Millisecond);
            date2 = CheckValues[ChangeType.Update.ToString()].Item2.datetimeColumn.GetValueOrDefault().AddMilliseconds(-CheckValues[ChangeType.Update.ToString()].Item2.datetimeColumn.GetValueOrDefault().Millisecond);
            Assert.AreEqual(date1.ToString("yyyyMMddhhmm"), date2.ToString("yyyyMMddhhmm")); Assert.IsNull(CheckValues[ChangeType.Delete.ToString()].Item2.datetime2Column);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.datetimeoffsetColumn.GetValueOrDefault().ToString("yyyyMMddhhmm"), CheckValues[ChangeType.Delete.ToString()].Item1.datetimeoffsetColumn.GetValueOrDefault().ToString("yyyyMMddhhmm"));

        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<DateTypeTestModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.dateColumn = e.Entity.dateColumn;
                    CheckValues[ChangeType.Insert.ToString()].Item2.datetimeColumn = e.Entity.datetimeColumn;
                    CheckValues[ChangeType.Insert.ToString()].Item2.datetime2Column = e.Entity.datetime2Column;
                    CheckValues[ChangeType.Insert.ToString()].Item2.datetimeoffsetColumn = e.Entity.datetimeoffsetColumn;
                    break;
                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.dateColumn = e.Entity.dateColumn;
                    CheckValues[ChangeType.Update.ToString()].Item2.datetimeColumn = e.Entity.datetimeColumn;
                    CheckValues[ChangeType.Update.ToString()].Item2.datetime2Column = e.Entity.datetime2Column;
                    CheckValues[ChangeType.Update.ToString()].Item2.datetimeoffsetColumn = e.Entity.datetimeoffsetColumn;
                    break;
                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.dateColumn = e.Entity.dateColumn;
                    CheckValues[ChangeType.Delete.ToString()].Item2.datetimeColumn = e.Entity.datetimeColumn;
                    CheckValues[ChangeType.Delete.ToString()].Item2.datetime2Column = e.Entity.datetime2Column;
                    CheckValues[ChangeType.Delete.ToString()].Item2.datetimeoffsetColumn = e.Entity.datetimeoffsetColumn;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<DateTypeTestModel, DateTypeTestModel>(new DateTypeTestModel { dateColumn =  DateTime.Now.AddDays(-1).Date, datetimeColumn = null, datetime2Column = DateTime.Now.AddDays(-3), datetimeoffsetColumn = DateTimeOffset.Now.AddDays(-4) }, new DateTypeTestModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<DateTypeTestModel, DateTypeTestModel>(new DateTypeTestModel { dateColumn = null, datetimeColumn = DateTime.Now, datetime2Column = null, datetimeoffsetColumn = DateTime.Now }, new DateTypeTestModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<DateTypeTestModel, DateTypeTestModel>(new DateTypeTestModel { dateColumn = null, datetimeColumn = DateTime.Now, datetime2Column = null, datetimeoffsetColumn = DateTime.Now }, new DateTypeTestModel()));

            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([dateColumn], [datetimeColumn], [datetime2Column], [datetimeoffsetColumn]) VALUES(@dateColumn, NULL, @datetime2Column, @datetimeoffsetColumn)";
                    sqlCommand.Parameters.Add(new SqlParameter("@dateColumn", SqlDbType.Date) { Value = CheckValues[ChangeType.Insert.ToString()].Item1.dateColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@datetime2Column", SqlDbType.DateTime2) { Value = CheckValues[ChangeType.Insert.ToString()].Item1.datetime2Column });
                    sqlCommand.Parameters.Add(new SqlParameter("@datetimeoffsetColumn", SqlDbType.DateTimeOffset) { Value = CheckValues[ChangeType.Insert.ToString()].Item1.datetimeoffsetColumn });
                    sqlCommand.ExecuteNonQuery();
                }

                Thread.Sleep(1000);

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [dateColumn] = NULL, [datetimeColumn] = @datetimeColumn, [datetime2Column] = NULL, [datetimeoffsetColumn] = @datetimeoffsetColumn";
                    sqlCommand.Parameters.Add(new SqlParameter("@datetimeColumn", SqlDbType.DateTime) { Value = CheckValues[ChangeType.Update.ToString()].Item1.datetimeColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@datetimeoffsetColumn", SqlDbType.DateTimeOffset) { Value = CheckValues[ChangeType.Update.ToString()].Item1.datetimeoffsetColumn });
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