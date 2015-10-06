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
using TableDependency.SqlClient.IntegrationTest.Model;

namespace TableDependency.SqlClient.IntegrationTest.Issues
{
    [TestClass]
    public class Issue_0007
    {
        private static string _connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private const string TableName = "Issue_0007";
        private static int _counter;
        private static Dictionary<string, Tuple<Issue_0007_Model, Issue_0007_Model>> _checkValues = new Dictionary<string, Tuple<Issue_0007_Model, Issue_0007_Model>>();

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

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}] ([testfloat] [float] NULL)";
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
        public void Test()
        {
            using (var sqlTableDependency = new SqlTableDependency<Issue_0007_Model>(_connectionString, TableName))
            {
                sqlTableDependency.OnChanged += this.SqlTableDependency_OnChanged;
                sqlTableDependency.OnError += this.SqlTableDependency_OnError;
                sqlTableDependency.Start();

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000);
            }

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.testfloat, _checkValues[ChangeType.Insert.ToString()].Item1.testfloat);

            Assert.IsTrue(_checkValues[ChangeType.Update.ToString()].Item2.testfloat == null);
            Assert.IsTrue(_checkValues[ChangeType.Update.ToString()].Item1.testfloat == null);

            Assert.IsTrue(_checkValues[ChangeType.Delete.ToString()].Item2.testfloat == null);
            Assert.IsTrue(_checkValues[ChangeType.Delete.ToString()].Item1.testfloat == null);

            Assert.IsTrue(_counter == 3);
        }

        private void SqlTableDependency_OnError(object sender, ErrorEventArgs e)
        {
            throw e.Error;
        }

        private void SqlTableDependency_OnChanged(object sender, RecordChangedEventArgs<Issue_0007_Model> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.testfloat = e.Entity.testfloat;
                    break;
                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.testfloat = e.Entity.testfloat;
                    break;
                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.testfloat = e.Entity.testfloat;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<Issue_0007_Model, Issue_0007_Model>(new Issue_0007_Model { testfloat = 123.4F }, new Issue_0007_Model()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<Issue_0007_Model, Issue_0007_Model>(new Issue_0007_Model { testfloat = null }, new Issue_0007_Model()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<Issue_0007_Model, Issue_0007_Model>(new Issue_0007_Model { testfloat = null }, new Issue_0007_Model()));

            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] (testfloat) VALUES (@testfloat)";
                    sqlCommand.Parameters.Add(new SqlParameter("@testfloat", SqlDbType.Float) {Value = _checkValues[ChangeType.Insert.ToString()].Item1.testfloat});
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);
                }
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [testfloat] = null";
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