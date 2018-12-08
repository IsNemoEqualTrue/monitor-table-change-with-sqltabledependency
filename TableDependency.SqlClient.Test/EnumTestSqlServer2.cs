using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class EnumTestSqlServer2 : Base.SqlTableDependencyBaseTest
    {
        public enum TestType
        {
            None = 0,
            UnitTest,
            IntegrationTest
        }

        private enum TestStatus : byte
        {
            None,
            Pass,
            Fail
        }

        public enum TesterName
        {
            DonalDuck,
            MickeyMouse
        }

        private class EnumTestSqlServerModel2
        {
            public string ErrorMessage { get; set; }
            public TesterName TesterName { get; set; }
            public TestType TestType { get; set; }
            public TestStatus TestStatus { get; set; }
        }

        private static readonly string TableName = typeof(EnumTestSqlServerModel2).Name.ToUpper();
        private static int _counter;
        private static readonly Dictionary<string, Tuple<EnumTestSqlServerModel2, EnumTestSqlServerModel2>> CheckValues = new Dictionary<string, Tuple<EnumTestSqlServerModel2, EnumTestSqlServerModel2>>();

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}]";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([TesterName] nvarchar(30), [TestType] int, [TestStatus] AS (CASE WHEN LTRIM(RTRIM(ISNULL([ErrorMessage],'')))='' THEN 'Pass' ELSE 'Fail' END) PERSISTED NOT NULL, [ErrorMessage] [NVARCHAR](512) NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
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
            SqlTableDependency<EnumTestSqlServerModel2> tableDependency = null;

            try
            {
                tableDependency = new SqlTableDependency<EnumTestSqlServerModel2>(ConnectionStringForTestUser, TableName);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.TesterName, CheckValues[ChangeType.Insert.ToString()].Item1.TesterName);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.TestStatus, CheckValues[ChangeType.Insert.ToString()].Item1.TestStatus);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.TestType, CheckValues[ChangeType.Insert.ToString()].Item1.TestType);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.ErrorMessage, CheckValues[ChangeType.Insert.ToString()].Item1.ErrorMessage);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.TesterName, CheckValues[ChangeType.Update.ToString()].Item1.TesterName);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.TestType, CheckValues[ChangeType.Update.ToString()].Item1.TestType);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.TestStatus, CheckValues[ChangeType.Update.ToString()].Item1.TestStatus);
            Assert.IsNull(CheckValues[ChangeType.Update.ToString()].Item2.ErrorMessage);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.TesterName, CheckValues[ChangeType.Delete.ToString()].Item1.TesterName);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.TestType, CheckValues[ChangeType.Delete.ToString()].Item1.TestType);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.TestStatus, CheckValues[ChangeType.Delete.ToString()].Item1.TestStatus);
            Assert.IsNull(CheckValues[ChangeType.Delete.ToString()].Item2.ErrorMessage);

            Assert.IsTrue(base.AreAllDbObjectDisposed(tableDependency.DataBaseObjectsNamingConvention));
            Assert.IsTrue(base.CountConversationEndpoints(tableDependency.DataBaseObjectsNamingConvention) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<EnumTestSqlServerModel2> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.TesterName = e.Entity.TesterName;
                    CheckValues[ChangeType.Insert.ToString()].Item2.TestType = e.Entity.TestType;
                    CheckValues[ChangeType.Insert.ToString()].Item2.TestStatus = e.Entity.TestStatus;
                    CheckValues[ChangeType.Insert.ToString()].Item2.ErrorMessage = e.Entity.ErrorMessage;
                    break;

                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.TesterName = e.Entity.TesterName;
                    CheckValues[ChangeType.Update.ToString()].Item2.TestType = e.Entity.TestType;
                    CheckValues[ChangeType.Update.ToString()].Item2.TestStatus = e.Entity.TestStatus;
                    CheckValues[ChangeType.Update.ToString()].Item2.ErrorMessage = e.Entity.ErrorMessage;
                    break;

                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.TesterName = e.Entity.TesterName;
                    CheckValues[ChangeType.Delete.ToString()].Item2.TestType = e.Entity.TestType;
                    CheckValues[ChangeType.Delete.ToString()].Item2.TestStatus = e.Entity.TestStatus;
                    CheckValues[ChangeType.Delete.ToString()].Item2.ErrorMessage = e.Entity.ErrorMessage;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<EnumTestSqlServerModel2, EnumTestSqlServerModel2>(new EnumTestSqlServerModel2 { TesterName = TesterName.DonalDuck, TestType = TestType.IntegrationTest, TestStatus = TestStatus.Fail, ErrorMessage = "Random error" }, new EnumTestSqlServerModel2()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<EnumTestSqlServerModel2, EnumTestSqlServerModel2>(new EnumTestSqlServerModel2 { TesterName = TesterName.MickeyMouse, TestType = TestType.UnitTest, TestStatus = TestStatus.Pass, ErrorMessage = null }, new EnumTestSqlServerModel2()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<EnumTestSqlServerModel2, EnumTestSqlServerModel2>(new EnumTestSqlServerModel2 { TesterName = TesterName.MickeyMouse, TestType = TestType.UnitTest, TestStatus = TestStatus.Pass, ErrorMessage = null }, new EnumTestSqlServerModel2()));

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([TesterName], [TestType], [ErrorMessage]) VALUES (N'{CheckValues[ChangeType.Insert.ToString()].Item1.TesterName}', {CheckValues[ChangeType.Insert.ToString()].Item1.TestType.GetHashCode()}, N'{CheckValues[ChangeType.Insert.ToString()].Item1.ErrorMessage}')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [ErrorMessage] = null, [TesterName] = N'{CheckValues[ChangeType.Update.ToString()].Item1.TesterName}', [TestType] = {CheckValues[ChangeType.Update.ToString()].Item1.TestType.GetHashCode()}";
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