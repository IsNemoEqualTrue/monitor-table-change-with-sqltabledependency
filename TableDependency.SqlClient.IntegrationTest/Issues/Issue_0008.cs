using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient.IntegrationTest.Model;

namespace TableDependency.SqlClient.IntegrationTest.Issues
{
    [TestClass]
    public class Issue_0008
    {
        private static string _connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private const string TableName = "Issue_0008";
        private static int _counter;
        private static Dictionary<string, Tuple<Issue_0008_Model, Issue_0008_Model>> _checkValues = new Dictionary<string, Tuple<Issue_0008_Model, Issue_0008_Model>>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}]";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}] (" +
                        "dateColumn date NULL, " +
                        "datetimeColumn datetime NULL, " +
                        "datetime2Column datetime2(7) NULL, " +
                        "datetimeoffsetColumn datetimeoffset(7) NULL, " +
                        "decimal54Column decimal(6, 4) NULL, " +
                        "floatColumn float NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize()]
        public void TestInitialize()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
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
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        public TestContext TestContext { get; set; }

        [TestMethod]
        public void DecimalAndFloatTest()
        {
            var restoreCulture = Thread.CurrentThread.CurrentCulture;
            var restoreUICulture = Thread.CurrentThread.CurrentUICulture;

            try
            {
                // Arrange
                Thread.CurrentThread.CurrentCulture = new CultureInfo("pl-PL");
                Thread.CurrentThread.CurrentUICulture = new CultureInfo("pl-PL");

                _checkValues.Clear();
                _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<Issue_0008_Model, Issue_0008_Model>(new Issue_0008_Model { floatColumn = 13.4F, decimal54Column = 12.45M }, new Issue_0008_Model()));
                _checkValues.Add(ChangeType.Update.ToString(), new Tuple<Issue_0008_Model, Issue_0008_Model>(new Issue_0008_Model { floatColumn = 44.11F, decimal54Column = 77.11M }, new Issue_0008_Model()));
                _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<Issue_0008_Model, Issue_0008_Model>(new Issue_0008_Model { floatColumn = 44.11F, decimal54Column = 77.11M }, new Issue_0008_Model()));


                // Act
                using (var sqlTableDependency = new SqlTableDependency<Issue_0008_Model>(_connectionString, TableName))
                {
                    sqlTableDependency.OnChanged += this.SqlTableDependency_OnChanged;
                    sqlTableDependency.OnError += this.SqlTableDependency_OnError;
                    sqlTableDependency.Start();

                    var t = new Task(ModifyTableContentForDecimalAndFloat);
                    t.Start();
                    t.Wait(20000);
                }


                // Assert
                Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.floatColumn, _checkValues[ChangeType.Insert.ToString()].Item1.floatColumn);
                Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.decimal54Column, _checkValues[ChangeType.Insert.ToString()].Item1.decimal54Column);

                Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.floatColumn, _checkValues[ChangeType.Update.ToString()].Item1.floatColumn);
                Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.decimal54Column, _checkValues[ChangeType.Update.ToString()].Item1.decimal54Column);

                Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.floatColumn, _checkValues[ChangeType.Delete.ToString()].Item1.floatColumn);
                Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.decimal54Column, _checkValues[ChangeType.Delete.ToString()].Item1.decimal54Column);
            }
            finally
            {
                Thread.CurrentThread.CurrentCulture = restoreCulture;
                Thread.CurrentThread.CurrentUICulture = restoreUICulture;
            }
        }

        private void SqlTableDependency_OnError(object sender, ErrorEventArgs e)
        {
            throw e.Error;
        }

        private void SqlTableDependency_OnChanged(object sender, RecordChangedEventArgs<Issue_0008_Model> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.floatColumn = e.Entity.floatColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.decimal54Column = e.Entity.decimal54Column;
                    _checkValues[ChangeType.Insert.ToString()].Item2.dateColumn = e.Entity.dateColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.datetimeColumn = e.Entity.datetimeColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.datetime2Column = e.Entity.datetime2Column;
                    _checkValues[ChangeType.Insert.ToString()].Item2.datetimeoffsetColumn = e.Entity.datetimeoffsetColumn;
                    break;
                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.floatColumn = e.Entity.floatColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.decimal54Column = e.Entity.decimal54Column;
                    _checkValues[ChangeType.Update.ToString()].Item2.dateColumn = e.Entity.dateColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.datetimeColumn = e.Entity.datetimeColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.datetime2Column = e.Entity.datetime2Column;
                    _checkValues[ChangeType.Update.ToString()].Item2.datetimeoffsetColumn = e.Entity.datetimeoffsetColumn;
                    break;
                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.floatColumn = e.Entity.floatColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.decimal54Column = e.Entity.decimal54Column;
                    _checkValues[ChangeType.Delete.ToString()].Item2.dateColumn = e.Entity.dateColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.datetimeColumn = e.Entity.datetimeColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.datetime2Column = e.Entity.datetime2Column;
                    _checkValues[ChangeType.Delete.ToString()].Item2.datetimeoffsetColumn = e.Entity.datetimeoffsetColumn;
                    break;
            }
        }

        private static void ModifyTableContentForDecimalAndFloat()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] (floatColumn, decimal54Column) VALUES (@floatColumn,@decimal54Column)";
                    sqlCommand.Parameters.Add(new SqlParameter("@floatColumn", SqlDbType.Float) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.floatColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@decimal54Column", SqlDbType.Decimal) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.decimal54Column });
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);
                }
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [floatColumn] = @floatColumn, [decimal54Column] = @decimal54Column";
                    sqlCommand.Parameters.Add(new SqlParameter("@floatColumn", SqlDbType.Float) { Value = _checkValues[ChangeType.Update.ToString()].Item1.floatColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@decimal54Column", SqlDbType.Decimal) { Value = _checkValues[ChangeType.Update.ToString()].Item1.decimal54Column });
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

        private static void ModifyTableContentForDates()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([dateColumn], [datetimeColumn], [datetime2Column], [datetimeoffsetColumn]) VALUES(@dateColumn, @datetimeColumn, @datetime2Column, @datetimeoffsetColumn)";
                    sqlCommand.Parameters.Add(new SqlParameter("@dateColumn", SqlDbType.Date) {Value = _checkValues[ChangeType.Insert.ToString()].Item1.dateColumn});
                    sqlCommand.Parameters.Add(new SqlParameter("@datetimeColumn", SqlDbType.DateTime) {Value = _checkValues[ChangeType.Insert.ToString()].Item1.datetimeColumn});
                    sqlCommand.Parameters.Add(new SqlParameter("@datetime2Column", SqlDbType.DateTime2) {Value = _checkValues[ChangeType.Insert.ToString()].Item1.datetime2Column});
                    sqlCommand.Parameters.Add(new SqlParameter("@datetimeoffsetColumn", SqlDbType.DateTimeOffset) {Value = _checkValues[ChangeType.Insert.ToString()].Item1.datetimeoffsetColumn});
                    sqlCommand.ExecuteNonQuery();
                }

                Thread.Sleep(1000);

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [dateColumn] = @dateColumn, [datetimeColumn] = @datetimeColumn, [datetime2Column] = @datetime2Column, [datetimeoffsetColumn] = @datetimeoffsetColumn";
                    sqlCommand.Parameters.Add(new SqlParameter("@dateColumn", SqlDbType.Date) {Value = _checkValues[ChangeType.Insert.ToString()].Item1.dateColumn});
                    sqlCommand.Parameters.Add(new SqlParameter("@datetimeColumn", SqlDbType.DateTime) {Value = _checkValues[ChangeType.Update.ToString()].Item1.datetimeColumn});
                    sqlCommand.Parameters.Add(new SqlParameter("@datetime2Column", SqlDbType.DateTime2) {Value = _checkValues[ChangeType.Insert.ToString()].Item1.datetime2Column});
                    sqlCommand.Parameters.Add(new SqlParameter("@datetimeoffsetColumn", SqlDbType.DateTimeOffset) {Value = _checkValues[ChangeType.Update.ToString()].Item1.datetimeoffsetColumn});
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