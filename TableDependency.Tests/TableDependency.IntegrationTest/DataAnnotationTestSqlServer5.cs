using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Base;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    [Table("ANItemsTableSQL5")]
    public class DataAnnotationTestSqlServer5Model
    {
        public long Id { get; set; }

        public string Name { get; set; }

        [Column("Long Description")]
        public string Description { get; set; }
    }

    [TestClass]
    public class DataAnnotationTestSqlServer5 : SqlTableDependencyBaseTest
    {
        private static int _counter;
        private static readonly Dictionary<string, Tuple<DataAnnotationTestSqlServer5Model, DataAnnotationTestSqlServer5Model>> CheckValues = new Dictionary<string, Tuple<DataAnnotationTestSqlServer5Model, DataAnnotationTestSqlServer5Model>>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = "IF OBJECT_ID('ANItemsTableSQL5', 'U') IS NOT NULL DROP TABLE [ANItemsTableSQL5];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = "CREATE TABLE [ANItemsTableSQL5]([Id] [int] IDENTITY(1, 1) NOT NULL, [Name] [NVARCHAR](50) NULL, [Long Description] [NVARCHAR](MAX) NULL)";
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
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = "IF OBJECT_ID('ANItemsTableSQL5', 'U') IS NOT NULL DROP TABLE [ANItemsTableSQL5];";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void EventForAllColumnsTest()
        {
            SqlTableDependency<DataAnnotationTestSqlServer5Model> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<DataAnnotationTestSqlServer5Model>(ConnectionStringForTestUser);
                tableDependency.OnChanged += TableDependency_Changed;                
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 10 * 1);
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

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming)== 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<DataAnnotationTestSqlServer5Model> e)
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
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<DataAnnotationTestSqlServer5Model, DataAnnotationTestSqlServer5Model>(new DataAnnotationTestSqlServer5Model { Name = "Christian", Description = "Del Bianco" }, new DataAnnotationTestSqlServer5Model()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<DataAnnotationTestSqlServer5Model, DataAnnotationTestSqlServer5Model>(new DataAnnotationTestSqlServer5Model { Name = "Velia", Description = "Ceccarelli" }, new DataAnnotationTestSqlServer5Model()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<DataAnnotationTestSqlServer5Model, DataAnnotationTestSqlServer5Model>(new DataAnnotationTestSqlServer5Model { Name = "Velia", Description = "Ceccarelli" }, new DataAnnotationTestSqlServer5Model()));

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [ANItemsTableSQL5] ([Name], [Long Description]) VALUES ('{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', '{CheckValues[ChangeType.Insert.ToString()].Item1.Description}')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"UPDATE [ANItemsTableSQL5] SET [Name] = '{CheckValues[ChangeType.Update.ToString()].Item1.Name}', [Long Description] = '{CheckValues[ChangeType.Update.ToString()].Item1.Description}'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = "DELETE FROM [ANItemsTableSQL5]";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);
                }
            }
        }
    }
}