using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.SqlServer;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    public class MoChangModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }        
    }

    [TestClass]
    public class NoChangesDuringFirstThreeMinutesTestSqlServer
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServer2008 Test_User"].ConnectionString;
        private static readonly string TableName = "MoChangModel";
        private static Dictionary<string, Tuple<MoChangModel, MoChangModel>> CheckValues = new Dictionary<string, Tuple<MoChangModel, MoChangModel>>();
        private static int _counter = 0;

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

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [int] NOT NULL, [Name] [NVARCHAR](50) NULL, [Surname] [NVARCHAR](MAX) NULL)";
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

        [TestCategory("SqlServer")]
        [TestMethod]
        public void AfterThreeMinutesICanGetNotifications()
        {
            SqlTableDependency<MoChangModel> tableDependency = null;
            string dataBaseObjectsNamingConvention = null;

            try
            {
                tableDependency = new SqlTableDependency<MoChangModel>(ConnectionString);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                dataBaseObjectsNamingConvention = tableDependency.DataBaseObjectsNamingConvention;
                
                Thread.Sleep(4 * 60 * 1000);
                Assert.IsFalse(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, dataBaseObjectsNamingConvention));

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }


            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, "Pizza Mergherita");
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, "Pizza Funghi");
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Surname, "Pizza Mergherita");

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, dataBaseObjectsNamingConvention));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<MoChangModel> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Surname = e.Entity.Surname;
                    break;
                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Surname = e.Entity.Surname;
                    break;
                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Update.ToString()].Item2.Surname = e.Entity.Surname;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<MoChangModel, MoChangModel>(new MoChangModel { Id = 23, Name = "Pizza Mergherita", Surname = "Pizza Mergherita" }, new MoChangModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<MoChangModel, MoChangModel>(new MoChangModel { Id = 23, Name = "Pizza Funghi", Surname = "Pizza Mergherita" }, new MoChangModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<MoChangModel, MoChangModel>(new MoChangModel { Id = 23, Name = "Pizza Funghi", Surname = "Pizza Funghi" }, new MoChangModel()));

            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name], [Surname]) VALUES ({CheckValues[ChangeType.Insert.ToString()].Item1.Id}, '{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', '{CheckValues[ChangeType.Insert.ToString()].Item1.Surname}')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = '{CheckValues[ChangeType.Update.ToString()].Item1.Name}'";
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