using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Exceptions;
using TableDependency.IntegrationTest.Helpers.SqlServer;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    public class ANItemsTableSQL8
    {
        public long IdNotExist { get; set; }
        public string NameNotExist { get; set; }
        public string DescriptionNotExist { get; set; }
    }

    [TestClass]
    public class DataAnnotationTestSqlServer8
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServer2008 Test_User"].ConnectionString;
        private static readonly string TableName = "ANItemsTableSQL8";
        private static int _counter;
        private static readonly Dictionary<string, Tuple<ANItemsTableSQL8, ANItemsTableSQL8>> CheckValues = new Dictionary<string, Tuple<ANItemsTableSQL8, ANItemsTableSQL8>>();

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

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [int] IDENTITY(1, 1) NOT NULL, [Name] [NVARCHAR](50) NULL, [Long Description] [NVARCHAR](MAX) NULL)";
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

        [TestCategory("SqlServer")]
        [TestMethod]
        [ExpectedException(typeof(NoMatchBetweenModelAndTableColumns))]
        public void EventForAllColumnsTest()
        {
            SqlTableDependency<ANItemsTableSQL8> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<ANItemsTableSQL8>(ConnectionString);
                tableDependency.OnChanged += TableDependency_Changed;
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

            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.NameNotExist, CheckValues[ChangeType.Insert.ToString()].Item1.NameNotExist);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.DescriptionNotExist, CheckValues[ChangeType.Insert.ToString()].Item1.DescriptionNotExist);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.NameNotExist, CheckValues[ChangeType.Update.ToString()].Item1.NameNotExist);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.DescriptionNotExist, CheckValues[ChangeType.Update.ToString()].Item1.DescriptionNotExist);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.NameNotExist, CheckValues[ChangeType.Delete.ToString()].Item1.NameNotExist);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.DescriptionNotExist, CheckValues[ChangeType.Delete.ToString()].Item1.DescriptionNotExist);

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(SqlServerHelper.AreAllEndpointDisposed(naming));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<ANItemsTableSQL8> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.NameNotExist = e.Entity.NameNotExist;
                    CheckValues[ChangeType.Insert.ToString()].Item2.DescriptionNotExist = e.Entity.DescriptionNotExist;
                    break;
                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.NameNotExist = e.Entity.NameNotExist;
                    CheckValues[ChangeType.Update.ToString()].Item2.DescriptionNotExist = e.Entity.DescriptionNotExist;
                    break;
                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.NameNotExist = e.Entity.NameNotExist;
                    CheckValues[ChangeType.Delete.ToString()].Item2.DescriptionNotExist = e.Entity.DescriptionNotExist;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<ANItemsTableSQL8, ANItemsTableSQL8>(new ANItemsTableSQL8 { NameNotExist = "Christian", DescriptionNotExist = "Del Bianco" }, new ANItemsTableSQL8()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<ANItemsTableSQL8, ANItemsTableSQL8>(new ANItemsTableSQL8 { NameNotExist = "Velia", DescriptionNotExist = "Ceccarelli" }, new ANItemsTableSQL8()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<ANItemsTableSQL8, ANItemsTableSQL8>(new ANItemsTableSQL8 { NameNotExist = "Velia", DescriptionNotExist = "Ceccarelli" }, new ANItemsTableSQL8()));

            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name], [Long Description]) VALUES ('{CheckValues[ChangeType.Insert.ToString()].Item1.NameNotExist}', '{CheckValues[ChangeType.Insert.ToString()].Item1.DescriptionNotExist}')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = '{CheckValues[ChangeType.Update.ToString()].Item1.NameNotExist}', [Long Description] = '{CheckValues[ChangeType.Update.ToString()].Item1.DescriptionNotExist}'";
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