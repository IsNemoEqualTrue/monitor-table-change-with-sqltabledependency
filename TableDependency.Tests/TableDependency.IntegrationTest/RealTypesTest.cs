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

namespace TableDependency.IntegrationTest
{
    public class CheckRealTypesModel
    {
        public float realColumn { get; set; }
    }

    [TestClass]
    public class RealTypesTestSqlServer
    {
        private static string _connectionString = ConfigurationManager.ConnectionStrings["SqlServerConnectionString"].ConnectionString;
        private static string TableName = "Real";
        private static Dictionary<string, Tuple<CheckRealTypesModel, CheckRealTypesModel>> _checkValues = new Dictionary<string, Tuple<CheckRealTypesModel, CheckRealTypesModel>>();

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

                    sqlCommand.CommandText = $"CREATE TABLE {TableName} (realColumn real NULL)";

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

        [TestCategory("SqlServer")]
        [TestMethod]
        public void Test()
        {
            SqlTableDependency<CheckRealTypesModel> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<CheckRealTypesModel>(_connectionString, TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
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

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.realColumn, _checkValues[ChangeType.Insert.ToString()].Item1.realColumn);

            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.realColumn, _checkValues[ChangeType.Update.ToString()].Item1.realColumn);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.realColumn, _checkValues[ChangeType.Delete.ToString()].Item1.realColumn);

        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<CheckRealTypesModel> e)
        {

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.realColumn = e.Entity.realColumn;
                    break;
                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.realColumn = e.Entity.realColumn;
                    break;
                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.realColumn = e.Entity.realColumn;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<CheckRealTypesModel, CheckRealTypesModel>(new CheckRealTypesModel {realColumn = 13}, new CheckRealTypesModel()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<CheckRealTypesModel, CheckRealTypesModel>(new CheckRealTypesModel {realColumn = 12}, new CheckRealTypesModel()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<CheckRealTypesModel, CheckRealTypesModel>(new CheckRealTypesModel {realColumn = 12}, new CheckRealTypesModel()));

            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([realColumn]) VALUES (@realColumn)";
                    sqlCommand.Parameters.Add(new SqlParameter("@realColumn", SqlDbType.Real) {Value = _checkValues[ChangeType.Insert.ToString()].Item1.realColumn});
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [realColumn] = @realColumn";
                    sqlCommand.Parameters.Add(new SqlParameter("@realColumn", SqlDbType.Real) {Value = _checkValues[ChangeType.Update.ToString()].Item1.realColumn});
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