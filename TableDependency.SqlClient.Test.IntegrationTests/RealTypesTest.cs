using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient.Test.Base;

namespace TableDependency.SqlClient.Test.IntegrationTests
{
    public class RealTypesTestSqlServerModel
    {
        public float RealColumn { get; set; }
    }

    [TestClass]
    public class RealTypesTestSqlServer : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(RealTypesTestSqlServerModel).Name;
        private static readonly Dictionary<string, Tuple<RealTypesTestSqlServerModel, RealTypesTestSqlServerModel>> CheckValues = new Dictionary<string, Tuple<RealTypesTestSqlServerModel, RealTypesTestSqlServerModel>>();

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

                    sqlCommand.CommandText = $"CREATE TABLE {TableName} (realColumn real NULL)";

                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize]
        public void TestInitialize()
        {
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
            SqlTableDependency<RealTypesTestSqlServerModel> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<RealTypesTestSqlServerModel>(ConnectionStringForTestUser);
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

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.RealColumn, CheckValues[ChangeType.Insert.ToString()].Item1.RealColumn);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.RealColumn, CheckValues[ChangeType.Update.ToString()].Item1.RealColumn);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.RealColumn, CheckValues[ChangeType.Delete.ToString()].Item1.RealColumn);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming)== 0);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<RealTypesTestSqlServerModel> e)
        {

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.RealColumn = e.Entity.RealColumn;
                    break;
                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.RealColumn = e.Entity.RealColumn;
                    break;
                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.RealColumn = e.Entity.RealColumn;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<RealTypesTestSqlServerModel, RealTypesTestSqlServerModel>(new RealTypesTestSqlServerModel {RealColumn = 13}, new RealTypesTestSqlServerModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<RealTypesTestSqlServerModel, RealTypesTestSqlServerModel>(new RealTypesTestSqlServerModel {RealColumn = 12}, new RealTypesTestSqlServerModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<RealTypesTestSqlServerModel, RealTypesTestSqlServerModel>(new RealTypesTestSqlServerModel {RealColumn = 12}, new RealTypesTestSqlServerModel()));

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([realColumn]) VALUES (@realColumn)";
                    sqlCommand.Parameters.Add(new SqlParameter("@realColumn", SqlDbType.Real) {Value = CheckValues[ChangeType.Insert.ToString()].Item1.RealColumn});
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [realColumn] = @realColumn";
                    sqlCommand.Parameters.Add(new SqlParameter("@realColumn", SqlDbType.Real) {Value = CheckValues[ChangeType.Update.ToString()].Item1.RealColumn});
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