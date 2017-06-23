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
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    public class MoChangModel2
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
    }

    [TestClass]
    public class NoChangesDuringFirstThreeMinutesTestSqlServer2
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServer2008 Test_User"].ConnectionString;
        private static readonly string TableName = "MoChangModel2";
        private static Dictionary<string, Tuple<MoChangModel2, MoChangModel2>> CheckValues = new Dictionary<string, Tuple<MoChangModel2, MoChangModel2>>();
        private static int _counter = 0;
        private static string Mal = "First Time";

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
            SqlTableDependency<MoChangModel2> tableDependency = null;
            string dataBaseObjectsNamingConvention = null;

            try
            {
                tableDependency = new SqlTableDependency<MoChangModel2>(ConnectionString);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                dataBaseObjectsNamingConvention = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(4 * 60 * 1000);
                Assert.IsFalse(SqlServerHelper.AreAllDbObjectDisposed(dataBaseObjectsNamingConvention));

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(20000);

                Assert.AreEqual(_counter, 3);
                Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, "Pizza Mergherita" + Mal);
                Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Surname, "Pizza Mergherita" + Mal);
                Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, "Pizza Funghi" + Mal);
                Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Surname, "Pizza Mergherita" + Mal);
                Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, "Pizza Funghi" + Mal);
                Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Surname, "Pizza Mergherita" + Mal);

                Mal = "Second round";
                _counter = 0;
                CheckValues = new Dictionary<string, Tuple<MoChangModel2, MoChangModel2>>();

                Thread.Sleep(7 * 60 * 1000);
                t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(20000);

                Assert.AreEqual(_counter, 3);
                Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, "Pizza Mergherita" + Mal);
                Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Surname, "Pizza Mergherita" + Mal);
                Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, "Pizza Funghi" + Mal);
                Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Surname, "Pizza Mergherita" + Mal);
                Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, "Pizza Funghi" + Mal);
                Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Surname, "Pizza Mergherita" + Mal);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Thread.Sleep(5000);
            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(dataBaseObjectsNamingConvention));
            Assert.IsTrue(SqlServerHelper.AreAllEndpointDisposed(dataBaseObjectsNamingConvention));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<MoChangModel2> e)
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
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<MoChangModel2, MoChangModel2>(new MoChangModel2 { Id = 23, Name = "Pizza Mergherita" + Mal, Surname = "Pizza Mergherita" + Mal }, new MoChangModel2()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<MoChangModel2, MoChangModel2>(new MoChangModel2 { Id = 23, Name = "Pizza Funghi" + Mal, Surname = "Pizza Mergherita" + Mal }, new MoChangModel2()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<MoChangModel2, MoChangModel2>(new MoChangModel2 { Id = 23, Name = "Pizza Funghi" + Mal, Surname = "Pizza Funghi" + Mal }, new MoChangModel2()));

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