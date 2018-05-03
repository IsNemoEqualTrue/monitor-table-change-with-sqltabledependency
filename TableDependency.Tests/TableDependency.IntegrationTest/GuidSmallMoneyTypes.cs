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
    public class GuidSmallMoneyTypes : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(GuidSmallMoneyTypesModel).Name;
        private static readonly Dictionary<string, Tuple<GuidSmallMoneyTypesModel, GuidSmallMoneyTypesModel>> CheckValues = new Dictionary<string, Tuple<GuidSmallMoneyTypesModel, GuidSmallMoneyTypesModel>>();

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
                Thread.Sleep(1000 * 10 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.UniqueidentifierColumn, CheckValues[ChangeType.Insert.ToString()].Item1.UniqueidentifierColumn);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Time7Column, CheckValues[ChangeType.Insert.ToString()].Item1.Time7Column);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.TinyintColumn, CheckValues[ChangeType.Insert.ToString()].Item1.TinyintColumn);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.SmalldatetimeColumn, CheckValues[ChangeType.Insert.ToString()].Item1.SmalldatetimeColumn);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.SmallintColumn, CheckValues[ChangeType.Insert.ToString()].Item1.SmallintColumn);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.MoneyColumn, CheckValues[ChangeType.Insert.ToString()].Item1.MoneyColumn);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.SmallmoneyColumn, CheckValues[ChangeType.Insert.ToString()].Item1.SmallmoneyColumn);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.SmallintColumn, CheckValues[ChangeType.Update.ToString()].Item1.SmallintColumn);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Time7Column, CheckValues[ChangeType.Update.ToString()].Item1.Time7Column);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.TinyintColumn, CheckValues[ChangeType.Update.ToString()].Item1.TinyintColumn);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.SmalldatetimeColumn, CheckValues[ChangeType.Update.ToString()].Item1.SmalldatetimeColumn);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.SmallintColumn, CheckValues[ChangeType.Update.ToString()].Item1.SmallintColumn);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.MoneyColumn, CheckValues[ChangeType.Update.ToString()].Item1.MoneyColumn);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.SmallmoneyColumn, CheckValues[ChangeType.Update.ToString()].Item1.SmallmoneyColumn);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.SmallintColumn, CheckValues[ChangeType.Delete.ToString()].Item1.SmallintColumn);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Time7Column, CheckValues[ChangeType.Delete.ToString()].Item1.Time7Column);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.TinyintColumn, CheckValues[ChangeType.Delete.ToString()].Item1.TinyintColumn);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.SmalldatetimeColumn, CheckValues[ChangeType.Delete.ToString()].Item1.SmalldatetimeColumn);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.SmallintColumn, CheckValues[ChangeType.Delete.ToString()].Item1.SmallintColumn);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.MoneyColumn, CheckValues[ChangeType.Delete.ToString()].Item1.MoneyColumn);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.SmallmoneyColumn, CheckValues[ChangeType.Delete.ToString()].Item1.SmallmoneyColumn);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming)== 0);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<GuidSmallMoneyTypesModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.UniqueidentifierColumn = e.Entity.UniqueidentifierColumn;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Time7Column = e.Entity.Time7Column;
                    CheckValues[ChangeType.Insert.ToString()].Item2.TinyintColumn = e.Entity.TinyintColumn;
                    CheckValues[ChangeType.Insert.ToString()].Item2.SmalldatetimeColumn = e.Entity.SmalldatetimeColumn;
                    CheckValues[ChangeType.Insert.ToString()].Item2.SmallintColumn = e.Entity.SmallintColumn;
                    CheckValues[ChangeType.Insert.ToString()].Item2.MoneyColumn = e.Entity.MoneyColumn;
                    CheckValues[ChangeType.Insert.ToString()].Item2.SmallmoneyColumn = e.Entity.SmallmoneyColumn;
                    break;

                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.UniqueidentifierColumn = e.Entity.UniqueidentifierColumn;
                    CheckValues[ChangeType.Update.ToString()].Item2.Time7Column = e.Entity.Time7Column;
                    CheckValues[ChangeType.Update.ToString()].Item2.TinyintColumn = e.Entity.TinyintColumn;
                    CheckValues[ChangeType.Update.ToString()].Item2.SmalldatetimeColumn = e.Entity.SmalldatetimeColumn;
                    CheckValues[ChangeType.Update.ToString()].Item2.SmallintColumn = e.Entity.SmallintColumn;
                    CheckValues[ChangeType.Update.ToString()].Item2.MoneyColumn = e.Entity.MoneyColumn;
                    CheckValues[ChangeType.Update.ToString()].Item2.SmallmoneyColumn = e.Entity.SmallmoneyColumn;
                    break;

                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.UniqueidentifierColumn = e.Entity.UniqueidentifierColumn;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Time7Column = e.Entity.Time7Column;
                    CheckValues[ChangeType.Delete.ToString()].Item2.TinyintColumn = e.Entity.TinyintColumn;
                    CheckValues[ChangeType.Delete.ToString()].Item2.SmalldatetimeColumn = e.Entity.SmalldatetimeColumn;
                    CheckValues[ChangeType.Delete.ToString()].Item2.SmallintColumn = e.Entity.SmallintColumn;
                    CheckValues[ChangeType.Delete.ToString()].Item2.MoneyColumn = e.Entity.MoneyColumn;
                    CheckValues[ChangeType.Delete.ToString()].Item2.SmallmoneyColumn = e.Entity.SmallmoneyColumn;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            // https://msdn.microsoft.com/en-us/library/bb675168%28v=vs.110%29.aspx
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<GuidSmallMoneyTypesModel, GuidSmallMoneyTypesModel>(new GuidSmallMoneyTypesModel { UniqueidentifierColumn = Guid.NewGuid(), Time7Column = DateTime.Parse("23:59:59").TimeOfDay, TinyintColumn = 1, SmalldatetimeColumn  = DateTime.Now.Date, SmallintColumn = 1, MoneyColumn = 123.77M, SmallmoneyColumn = 2.3M }, new GuidSmallMoneyTypesModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<GuidSmallMoneyTypesModel, GuidSmallMoneyTypesModel>(new GuidSmallMoneyTypesModel { UniqueidentifierColumn = Guid.NewGuid(), Time7Column = DateTime.Parse("13:59:59").TimeOfDay, TinyintColumn = 2, SmalldatetimeColumn = DateTime.Now.Date.AddDays(1), SmallintColumn = 1, MoneyColumn = 23.77M, SmallmoneyColumn = 1.3M }, new GuidSmallMoneyTypesModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<GuidSmallMoneyTypesModel, GuidSmallMoneyTypesModel>(new GuidSmallMoneyTypesModel { UniqueidentifierColumn = CheckValues[ChangeType.Update.ToString()].Item2.UniqueidentifierColumn, Time7Column = DateTime.Parse("13:59:59").TimeOfDay, TinyintColumn = 2, SmalldatetimeColumn = DateTime.Now.Date.AddDays(1), SmallintColumn = 1, MoneyColumn = 23.77M, SmallmoneyColumn = 1.3M }, new GuidSmallMoneyTypesModel()));

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] (uniqueidentifierColumn, time7Column, tinyintColumn, smalldatetimeColumn, smallintColumn, moneyColumn, smallmoneyColumn) " +
                        "values (@uniqueidentifierColumn, @time7Column, @tinyintColumn, @smalldatetimeColumn, @smallintColumn, @moneyColumn, @smallmoneyColumn)";

                    sqlCommand.Parameters.Add(new SqlParameter("@uniqueidentifierColumn", SqlDbType.UniqueIdentifier) { Value = CheckValues[ChangeType.Insert.ToString()].Item1.UniqueidentifierColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@time7Column", SqlDbType.Time) { Value = CheckValues[ChangeType.Insert.ToString()].Item1.Time7Column });
                    sqlCommand.Parameters.Add(new SqlParameter("@tinyintColumn", SqlDbType.TinyInt) { Value = CheckValues[ChangeType.Insert.ToString()].Item1.TinyintColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@smalldatetimeColumn", SqlDbType.SmallDateTime) { Value = CheckValues[ChangeType.Insert.ToString()].Item1.SmalldatetimeColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@smallintColumn", SqlDbType.SmallInt) { Value = CheckValues[ChangeType.Insert.ToString()].Item1.SmallintColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@moneyColumn", SqlDbType.Money) { Value = CheckValues[ChangeType.Insert.ToString()].Item1.MoneyColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@smallmoneyColumn", SqlDbType.SmallMoney) { Value = CheckValues[ChangeType.Insert.ToString()].Item1.SmallmoneyColumn });

                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET uniqueidentifierColumn = @uniqueidentifierColumn, time7Column = @time7Column, tinyintColumn = @tinyintColumn, smalldatetimeColumn = @smalldatetimeColumn, smallintColumn = @smallintColumn, moneyColumn = @moneyColumn, smallmoneyColumn = @smallmoneyColumn";

                    sqlCommand.Parameters.Add(new SqlParameter("@uniqueidentifierColumn", SqlDbType.UniqueIdentifier) { Value = CheckValues[ChangeType.Update.ToString()].Item1.UniqueidentifierColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@time7Column", SqlDbType.Time) { Value = CheckValues[ChangeType.Update.ToString()].Item1.Time7Column });
                    sqlCommand.Parameters.Add(new SqlParameter("@tinyintColumn", SqlDbType.TinyInt) { Value = CheckValues[ChangeType.Update.ToString()].Item1.TinyintColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@smalldatetimeColumn", SqlDbType.SmallDateTime) { Value = CheckValues[ChangeType.Update.ToString()].Item1.SmalldatetimeColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@smallintColumn", SqlDbType.SmallInt) { Value = CheckValues[ChangeType.Update.ToString()].Item1.SmallintColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@moneyColumn", SqlDbType.Money) { Value = CheckValues[ChangeType.Update.ToString()].Item1.MoneyColumn });
                    sqlCommand.Parameters.Add(new SqlParameter("@smallmoneyColumn", SqlDbType.SmallMoney) { Value = CheckValues[ChangeType.Update.ToString()].Item1.SmallmoneyColumn });

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