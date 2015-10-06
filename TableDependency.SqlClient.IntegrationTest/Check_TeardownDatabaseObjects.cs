using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.SqlClient.IntegrationTest.Helpers;
using TableDependency.SqlClient.IntegrationTest.Model;

namespace TableDependency.SqlClient.IntegrationTest
{
    [TestClass]
    public class CheckTeardownDatabaseObjects
    {
        private static Dictionary<string, Tuple<Check_Model, Check_Model>> _checkValues = new Dictionary<string, Tuple<Check_Model, Check_Model>>();
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private static string TableName = "TeardownDatabaseObjects";

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

                    sqlCommand.CommandText =
                        $"CREATE TABLE [{TableName}](" +
                        "[Id] [INT] IDENTITY(1, 1) NOT NULL, " +
                        "[First Name] [NVARCHAR](MAX) NOT NULL, " +
                        "[Second Name] [NVARCHAR](MAX) NULL, " +
                        "[Born] [DATETIME] NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize()]
        public void TestInitialize()
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
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

        [TestMethod]
        public void StartAndStop()
        {
            SqlTableDependency<Check_Model> tableDependency = null;
            string dataBaseObjectsNamingConvention;

            var mapper = new ModelToTableMapper<Check_Model>();
            mapper.AddMapping(c => c.Name, "First Name").AddMapping(c => c.Surname, "Second Name");

            try
            {
                tableDependency = new SqlTableDependency<Check_Model>(ConnectionString, TableName, mapper, new List<string> { "First Name" }, false);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                dataBaseObjectsNamingConvention = tableDependency.DataBaseObjectsNamingConvention;
                Thread.Sleep(5000);
                tableDependency.Stop();
                Thread.Sleep(5000);

                Assert.IsFalse(Helper.AreAllDbObjectDisposed(ConnectionString, dataBaseObjectsNamingConvention));
                Thread.Sleep(5000);
                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(5000);

                tableDependency.Start();
                Thread.Sleep(5000);
                tableDependency.Stop(true);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Thread.Sleep(1 * 60 * 1000);

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Name, _checkValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Name, _checkValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Name, _checkValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.IsTrue(Helper.AreAllDbObjectDisposed(ConnectionString, dataBaseObjectsNamingConvention));
        }

        [TestMethod]
        public void PostEventReceivment()
        {
            SqlTableDependency<Check_Model> tableDependency = null;
            string dataBaseObjectsNamingConvention;

            var mapper = new ModelToTableMapper<Check_Model>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second Name");

            try
            {
                tableDependency = new SqlTableDependency<Check_Model>(ConnectionString, TableName, mapper, new List<string> { "First Name" }, false);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                dataBaseObjectsNamingConvention = tableDependency.DataBaseObjectsNamingConvention;
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.IsFalse(Helper.AreAllDbObjectDisposed(ConnectionString, dataBaseObjectsNamingConvention));
            Thread.Sleep(5000);
            var t = new Task(ModifyTableContent);
            t.Start();
            t.Wait(5000);

            try
            {
                tableDependency = new SqlTableDependency<Check_Model>(ConnectionString, dataBaseObjectsNamingConvention, true, mapper);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                Thread.Sleep(10000);
                tableDependency.Stop(true);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Name, _checkValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Name, _checkValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Name, _checkValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.IsTrue(Helper.AreAllDbObjectDisposed(ConnectionString, dataBaseObjectsNamingConvention));
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<Check_Model> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    break;
                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                    break;
                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<Check_Model, Check_Model>(new Check_Model { Name = "Christian" }, new Check_Model()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<Check_Model, Check_Model>(new Check_Model { Name = "Valentina" }, new Check_Model()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<Check_Model, Check_Model>(new Check_Model { Name = "Valentina" }, new Check_Model()));

            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name]) VALUES(@name)";
                    sqlCommand.Parameters.AddWithValue("@name", _checkValues[ChangeType.Insert.ToString()].Item1.Name);
                    sqlCommand.ExecuteNonQuery();
                }

                Thread.Sleep(500);

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = @name";
                    sqlCommand.Parameters.AddWithValue("@name", _checkValues[ChangeType.Update.ToString()].Item1.Name);
                    sqlCommand.ExecuteNonQuery();
                }

                Thread.Sleep(500);

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }

                Thread.Sleep(500);
            }
        }
    }
}