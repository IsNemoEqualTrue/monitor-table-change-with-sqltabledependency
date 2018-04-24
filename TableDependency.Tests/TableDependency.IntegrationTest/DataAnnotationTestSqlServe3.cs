using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
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
    [Table("ANItemsTableSQL3")]
    public class DataAnnotationTestSelServerModel3
    {
        public long Id { get; set; }

        public string Name { get; set; }

        [Column("XXX")]
        public string Description { get; set; }
    }

    [TestClass]
    public class DataAnnotationTestSqlServer3
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServer2008 Test_User"].ConnectionString;
        private static int _counter;
        private static readonly Dictionary<string, Tuple<DataAnnotationTestSelServerModel3, DataAnnotationTestSelServerModel3>> CheckValues = new Dictionary<string, Tuple<DataAnnotationTestSelServerModel3, DataAnnotationTestSelServerModel3>>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = "IF OBJECT_ID('ANItemsTableSQL3', 'U') IS NOT NULL DROP TABLE [ANItemsTableSQL3];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = "CREATE TABLE [ANItemsTableSQL3]([Id] [int] IDENTITY(1, 1) NOT NULL, [Name] [NVARCHAR](50) NULL, [Long Description] [NVARCHAR](MAX) NULL)";
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
                    sqlCommand.CommandText = "IF OBJECT_ID('ANItemsTableSQL3', 'U') IS NOT NULL DROP TABLE [ANItemsTableSQL3];";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        [ExpectedException(typeof(ModelToTableMapperException))]
        public void EventForAllColumnsTest()
        {
            SqlTableDependency<DataAnnotationTestSelServerModel3> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<DataAnnotationTestSelServerModel3>(ConnectionString);
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

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Description, CheckValues[ChangeType.Insert.ToString()].Item1.Description);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, CheckValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Description, CheckValues[ChangeType.Update.ToString()].Item1.Description);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Description, CheckValues[ChangeType.Delete.ToString()].Item1.Description);

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(SqlServerHelper.AreAllEndpointDisposed(naming));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<DataAnnotationTestSelServerModel3> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Description = e.Entity.Description;
                    break;
                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Update.ToString()].Item2.Description = e.Entity.Description;
                    break;
                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Description = e.Entity.Description;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<DataAnnotationTestSelServerModel3, DataAnnotationTestSelServerModel3>(new DataAnnotationTestSelServerModel3 { Name = "Christian", Description = "Del Bianco" }, new DataAnnotationTestSelServerModel3()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<DataAnnotationTestSelServerModel3, DataAnnotationTestSelServerModel3>(new DataAnnotationTestSelServerModel3 { Name = "Velia", Description = "Ceccarelli" }, new DataAnnotationTestSelServerModel3()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<DataAnnotationTestSelServerModel3, DataAnnotationTestSelServerModel3>(new DataAnnotationTestSelServerModel3 { Name = "Velia", Description = "Ceccarelli" }, new DataAnnotationTestSelServerModel3()));

            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [ANItemsTableSQL3] ([Name], [Long Description]) VALUES ('{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', '{CheckValues[ChangeType.Insert.ToString()].Item1.Description}')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"UPDATE [ANItemsTableSQL3] SET [Name] = '{CheckValues[ChangeType.Update.ToString()].Item1.Name}', [Long Description] = '{CheckValues[ChangeType.Update.ToString()].Item1.Description}'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = "DELETE FROM [ANItemsTableSQL3]";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);
                }
            }
        }
    }
}