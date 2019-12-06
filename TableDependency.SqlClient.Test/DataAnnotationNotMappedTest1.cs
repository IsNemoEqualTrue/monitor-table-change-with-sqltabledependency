using System; 
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class DataAnnotationNotMappedTest1 : Base.SqlTableDependencyBaseTest
    {
        [Table("DataAnnotationNotMappedTest1Model")]
        private class DataAnnotationNotMappedTest1Model
        {
            [NotMapped]
            public int Number { get => int.Parse(this.StringNumberInDatabase); set => this.StringNumberInDatabase = value.ToString(); }

            [Column("Number")]
            public string StringNumberInDatabase { get; set; }
        }
    
        private const string ScemaName = "[dbo]";
        private static int _counter;
        private static Dictionary<string, Tuple<DataAnnotationNotMappedTest1Model, DataAnnotationNotMappedTest1Model>> _checkValuesTest1 = new Dictionary<string, Tuple<DataAnnotationNotMappedTest1Model, DataAnnotationNotMappedTest1Model>>();

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = "IF OBJECT_ID('DataAnnotationNotMappedTest1Model', 'U') IS NOT NULL DROP TABLE [DataAnnotationNotMappedTest1Model];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = "CREATE TABLE [DataAnnotationNotMappedTest1Model]([Number] [NVARCHAR](50) NOT NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize]
        public void TestInitialize()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = "DELETE FROM [DataAnnotationNotMappedTest1Model];";
                    sqlCommand.ExecuteNonQuery();
                }
            }

            _checkValuesTest1.Clear();

            _counter = 0;

            _checkValuesTest1.Add(ChangeType.Insert.ToString(), new Tuple<DataAnnotationNotMappedTest1Model, DataAnnotationNotMappedTest1Model>(new DataAnnotationNotMappedTest1Model { StringNumberInDatabase = "100" }, new DataAnnotationNotMappedTest1Model()));
            _checkValuesTest1.Add(ChangeType.Update.ToString(), new Tuple<DataAnnotationNotMappedTest1Model, DataAnnotationNotMappedTest1Model>(new DataAnnotationNotMappedTest1Model { StringNumberInDatabase = "990" }, new DataAnnotationNotMappedTest1Model()));
            _checkValuesTest1.Add(ChangeType.Delete.ToString(), new Tuple<DataAnnotationNotMappedTest1Model, DataAnnotationNotMappedTest1Model>(new DataAnnotationNotMappedTest1Model { StringNumberInDatabase = "990" }, new DataAnnotationNotMappedTest1Model()));
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = "IF OBJECT_ID('DataAnnotationNotMappedTest1Model', 'U') IS NOT NULL DROP TABLE [DataAnnotationNotMappedTest1Model];";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void Test1()
        {
            SqlTableDependency<DataAnnotationNotMappedTest1Model> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<DataAnnotationNotMappedTest1Model>(
                    ConnectionStringForTestUser,
                    includeOldValues: false,
                    schemaName: ScemaName);

                tableDependency.OnChanged += TableDependency_Changed_Test1;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContentTest1);
                t.Start();
                Thread.Sleep(1000 * 5 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }


            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(_checkValuesTest1[ChangeType.Insert.ToString()].Item2.StringNumberInDatabase, _checkValuesTest1[ChangeType.Insert.ToString()].Item1.StringNumberInDatabase);
            Assert.AreEqual(_checkValuesTest1[ChangeType.Insert.ToString()].Item2.Number, int.Parse(_checkValuesTest1[ChangeType.Insert.ToString()].Item1.StringNumberInDatabase));

            Assert.AreEqual(_checkValuesTest1[ChangeType.Update.ToString()].Item2.StringNumberInDatabase, _checkValuesTest1[ChangeType.Update.ToString()].Item1.StringNumberInDatabase);
            Assert.AreEqual(_checkValuesTest1[ChangeType.Update.ToString()].Item2.Number, int.Parse(_checkValuesTest1[ChangeType.Update.ToString()].Item1.StringNumberInDatabase));

            Assert.AreEqual(_checkValuesTest1[ChangeType.Delete.ToString()].Item2.StringNumberInDatabase, _checkValuesTest1[ChangeType.Delete.ToString()].Item1.StringNumberInDatabase);
            Assert.AreEqual(_checkValuesTest1[ChangeType.Delete.ToString()].Item2.Number, int.Parse(_checkValuesTest1[ChangeType.Update.ToString()].Item1.StringNumberInDatabase));

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private static void TableDependency_Changed_Test1(object sender, RecordChangedEventArgs<DataAnnotationNotMappedTest1Model> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValuesTest1[ChangeType.Insert.ToString()].Item2.StringNumberInDatabase = e.Entity.StringNumberInDatabase;

                    break;

                case ChangeType.Update:
                    _checkValuesTest1[ChangeType.Update.ToString()].Item2.StringNumberInDatabase = e.Entity.StringNumberInDatabase;

                    break;

                case ChangeType.Delete:
                    _checkValuesTest1[ChangeType.Delete.ToString()].Item2.StringNumberInDatabase = e.Entity.StringNumberInDatabase;
                    
                    break;
            }
        }

        private static void ModifyTableContentTest1()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [DataAnnotationNotMappedTest1Model] ([Number]) VALUES ('{_checkValuesTest1[ChangeType.Insert.ToString()].Item1.StringNumberInDatabase}')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [DataAnnotationNotMappedTest1Model] SET [Number] = '{_checkValuesTest1[ChangeType.Update.ToString()].Item1.StringNumberInDatabase}'";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = "DELETE FROM [DataAnnotationNotMappedTest1Model]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}