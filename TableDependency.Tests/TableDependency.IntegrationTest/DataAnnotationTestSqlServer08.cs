using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Exceptions;
using TableDependency.IntegrationTest.Base;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    public class DataAnnotationTestSqlServer8Model
    {
        public long IdNotExist { get; set; }
        public string NameNotExist { get; set; }
        public string DescriptionNotExist { get; set; }
    }

    [TestClass]
    public class DataAnnotationTestSqlServer08 : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(DataAnnotationTestSqlServer8Model).Name;
        private static int _counter;
        private static readonly Dictionary<string, Tuple<DataAnnotationTestSqlServer8Model, DataAnnotationTestSqlServer8Model>> CheckValues = new Dictionary<string, Tuple<DataAnnotationTestSqlServer8Model, DataAnnotationTestSqlServer8Model>>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
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
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
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
            SqlTableDependency<DataAnnotationTestSqlServer8Model> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<DataAnnotationTestSqlServer8Model>(ConnectionStringForTestUser);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 5 * 1);
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

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<DataAnnotationTestSqlServer8Model> e)
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
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<DataAnnotationTestSqlServer8Model, DataAnnotationTestSqlServer8Model>(new DataAnnotationTestSqlServer8Model { NameNotExist = "Christian", DescriptionNotExist = "Del Bianco" }, new DataAnnotationTestSqlServer8Model()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<DataAnnotationTestSqlServer8Model, DataAnnotationTestSqlServer8Model>(new DataAnnotationTestSqlServer8Model { NameNotExist = "Velia", DescriptionNotExist = "Ceccarelli" }, new DataAnnotationTestSqlServer8Model()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<DataAnnotationTestSqlServer8Model, DataAnnotationTestSqlServer8Model>(new DataAnnotationTestSqlServer8Model { NameNotExist = "Velia", DescriptionNotExist = "Ceccarelli" }, new DataAnnotationTestSqlServer8Model()));

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name], [Long Description]) VALUES ('{CheckValues[ChangeType.Insert.ToString()].Item1.NameNotExist}', '{CheckValues[ChangeType.Insert.ToString()].Item1.DescriptionNotExist}')";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = '{CheckValues[ChangeType.Update.ToString()].Item1.NameNotExist}', [Long Description] = '{CheckValues[ChangeType.Update.ToString()].Item1.DescriptionNotExist}'";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}