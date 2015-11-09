using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.SqlServer;
using TableDependency.Mappers;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    public class DisposeAndRestartWithSameObjectsTestSqlServerModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
        public DateTime Born { get; set; }
        public int Quantity { get; set; }
    }

    [TestClass]
    public class DisposeAndRestartWithSameObjectsTestSqlServer
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServerConnectionString"].ConnectionString;
        private const string TableName = "AAAADisposeAndRestartWithSameObjects";
        private static int _counter;
        private static Dictionary<string, Tuple<DisposeAndRestartWithSameObjectsTestSqlServerModel, DisposeAndRestartWithSameObjectsTestSqlServerModel>> _checkValues = new Dictionary<string, Tuple<DisposeAndRestartWithSameObjectsTestSqlServerModel, DisposeAndRestartWithSameObjectsTestSqlServerModel>>();

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
                        $"CREATE TABLE [{TableName}]( " +
                        "[Id][int] IDENTITY(1, 1) NOT NULL, " +
                        "[First Name] [nvarchar](50) NULL, " +
                        "[Second Name] [nvarchar](50) NULL, " +
                        "[Born] [datetime] NULL)";
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

        [TestMethod]
        public void Test()
        {
            var namingToUse = "CustomNaming";

            var mapper = new ModelToTableMapper<DisposeAndRestartWithSameObjectsTestSqlServerModel>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second Name");

            using (var tableDependency = new SqlTableDependency<DisposeAndRestartWithSameObjectsTestSqlServerModel>(ConnectionString, TableName, mapper, false, namingToUse))
            {
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                Assert.AreEqual(tableDependency.DataBaseObjectsNamingConvention, namingToUse);
                Thread.Sleep(1 * 25 * 1000);
            }

            Thread.Sleep(1 * 60 * 1000);

            using (var tableDependency = new SqlTableDependency<DisposeAndRestartWithSameObjectsTestSqlServerModel>(ConnectionString, TableName, mapper, true, namingToUse))
            {
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                Assert.AreEqual(tableDependency.DataBaseObjectsNamingConvention, namingToUse);

                Thread.Sleep(1 * 25 * 1000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(1 * 60 * 1000);
            }

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, namingToUse));
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Name, _checkValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Surname, _checkValues[ChangeType.Insert.ToString()].Item1.Surname);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Name, _checkValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Surname, _checkValues[ChangeType.Update.ToString()].Item1.Surname);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Name, _checkValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Surname, _checkValues[ChangeType.Delete.ToString()].Item1.Surname);            
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<DisposeAndRestartWithSameObjectsTestSqlServerModel> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    _checkValues[ChangeType.Insert.ToString()].Item2.Surname = e.Entity.Surname;
                    break;
                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                    _checkValues[ChangeType.Update.ToString()].Item2.Surname = e.Entity.Surname;
                    break;
                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    _checkValues[ChangeType.Delete.ToString()].Item2.Surname = e.Entity.Surname;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<DisposeAndRestartWithSameObjectsTestSqlServerModel, DisposeAndRestartWithSameObjectsTestSqlServerModel>(new DisposeAndRestartWithSameObjectsTestSqlServerModel { Name = "Christian", Surname = "Del Bianco" }, new DisposeAndRestartWithSameObjectsTestSqlServerModel()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<DisposeAndRestartWithSameObjectsTestSqlServerModel, DisposeAndRestartWithSameObjectsTestSqlServerModel>(new DisposeAndRestartWithSameObjectsTestSqlServerModel { Name = "Velia", Surname = "Ceccarelli" }, new DisposeAndRestartWithSameObjectsTestSqlServerModel()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<DisposeAndRestartWithSameObjectsTestSqlServerModel, DisposeAndRestartWithSameObjectsTestSqlServerModel>(new DisposeAndRestartWithSameObjectsTestSqlServerModel { Name = "Velia", Surname = "Ceccarelli" }, new DisposeAndRestartWithSameObjectsTestSqlServerModel()));

            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{_checkValues[ChangeType.Insert.ToString()].Item1.Name}', '{_checkValues[ChangeType.Insert.ToString()].Item1.Surname}')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = '{_checkValues[ChangeType.Update.ToString()].Item1.Name}', [Second Name] = '{_checkValues[ChangeType.Update.ToString()].Item1.Surname}'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);
                }
            }
        }
    }
}