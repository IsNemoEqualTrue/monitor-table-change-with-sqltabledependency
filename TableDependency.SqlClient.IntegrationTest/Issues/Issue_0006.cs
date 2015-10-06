using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient.IntegrationTest.Helpers;
using TableDependency.SqlClient.IntegrationTest.Model;

namespace TableDependency.SqlClient.IntegrationTest.Issues
{
    [TestClass]
    public class Issue_0006
    {
        private static string _connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private const string TableName = "Issue_0006";
        private static int _counter;
        private static Dictionary<string, Tuple<Issue_0006_Model, Issue_0006_Model>> _checkValues = new Dictionary<string, Tuple<Issue_0006_Model, Issue_0006_Model>>();

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

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}](" +
                        "[Id] [INT] IDENTITY(1, 1) NOT NULL PRIMARY KEY, " +
                        "[ProcessedNullableWithDefault] [BIT] NULL DEFAULT 0," +
                        "[ProcessedNullable] [BIT] NULL," +
                        "[Processed] [BIT] NOT NULL)";
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
        public void ProblemWithBoolean()
        {
            using (var sqlTableDependency = new SqlTableDependency<Issue_0006_Model>(_connectionString, TableName))
            {
                sqlTableDependency.OnChanged += this.SqlTableDependency_OnChanged;
                sqlTableDependency.OnError += this.SqlTableDependency_OnError;
                sqlTableDependency.Start();

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000);
            }

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.ProcessedNullableWithDefault, _checkValues[ChangeType.Insert.ToString()].Item1.ProcessedNullableWithDefault);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.ProcessedNullable, _checkValues[ChangeType.Insert.ToString()].Item1.ProcessedNullable);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Processed, _checkValues[ChangeType.Insert.ToString()].Item1.Processed);

            Assert.IsTrue(_checkValues[ChangeType.Update.ToString()].Item2.ProcessedNullableWithDefault == null && _checkValues[ChangeType.Update.ToString()].Item1.ProcessedNullableWithDefault == null);
            Assert.IsTrue(_checkValues[ChangeType.Update.ToString()].Item2.ProcessedNullable == null && _checkValues[ChangeType.Update.ToString()].Item1.ProcessedNullable == null);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Processed, _checkValues[ChangeType.Insert.ToString()].Item1.Processed);

            Assert.IsTrue(_checkValues[ChangeType.Delete.ToString()].Item2.ProcessedNullableWithDefault == null && _checkValues[ChangeType.Delete.ToString()].Item1.ProcessedNullableWithDefault == null);
            Assert.IsTrue(_checkValues[ChangeType.Delete.ToString()].Item2.ProcessedNullable == null && _checkValues[ChangeType.Delete.ToString()].Item1.ProcessedNullable == null);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Processed, _checkValues[ChangeType.Insert.ToString()].Item1.Processed);

            Assert.IsTrue(_counter == 3);
        }

        private void SqlTableDependency_OnError(object sender, ErrorEventArgs e)
        {
            throw e.Error;
        }

        private void SqlTableDependency_OnChanged(object sender, RecordChangedEventArgs<Issue_0006_Model> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.ProcessedNullableWithDefault = e.Entity.ProcessedNullableWithDefault;
                    _checkValues[ChangeType.Insert.ToString()].Item2.ProcessedNullable = e.Entity.ProcessedNullable;
                    _checkValues[ChangeType.Update.ToString()].Item2.Processed = e.Entity.Processed;
                    break;
                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.ProcessedNullableWithDefault = e.Entity.ProcessedNullableWithDefault;
                    _checkValues[ChangeType.Update.ToString()].Item2.ProcessedNullable = e.Entity.ProcessedNullable;
                    _checkValues[ChangeType.Update.ToString()].Item2.Processed = e.Entity.Processed;
                    break;
                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.ProcessedNullableWithDefault = e.Entity.ProcessedNullableWithDefault;
                    _checkValues[ChangeType.Delete.ToString()].Item2.ProcessedNullable = e.Entity.ProcessedNullable;
                    _checkValues[ChangeType.Update.ToString()].Item2.Processed = e.Entity.Processed;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<Issue_0006_Model, Issue_0006_Model>(
                new Issue_0006_Model { ProcessedNullableWithDefault = true, ProcessedNullable = true, Processed = false }, new Issue_0006_Model()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<Issue_0006_Model, Issue_0006_Model>(
                new Issue_0006_Model { ProcessedNullableWithDefault = null, ProcessedNullable = null, Processed = true }, new Issue_0006_Model()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<Issue_0006_Model, Issue_0006_Model>(
                new Issue_0006_Model { ProcessedNullableWithDefault = null, ProcessedNullable = null, Processed = true }, new Issue_0006_Model()));

            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([ProcessedNullableWithDefault], [ProcessedNullable], [Processed]) " +
                        $"VALUES ({_checkValues[ChangeType.Insert.ToString()].Item1.ProcessedNullableWithDefault.ToNullableBit()}, {_checkValues[ChangeType.Insert.ToString()].Item1.ProcessedNullable.ToNullableBit()}, {_checkValues[ChangeType.Insert.ToString()].Item1.Processed.ToBit()})"; 
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET " + 
                        $"[ProcessedNullableWithDefault] = {_checkValues[ChangeType.Update.ToString()].Item1.ProcessedNullableWithDefault.ToNullableBit()}," +
                        $"[ProcessedNullable] = {_checkValues[ChangeType.Update.ToString()].Item1.ProcessedNullable.ToNullableBit()}," +
                        $"[Processed] = {_checkValues[ChangeType.Update.ToString()].Item1.Processed.ToBit()}";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);
                }
            }
        }
    }
}