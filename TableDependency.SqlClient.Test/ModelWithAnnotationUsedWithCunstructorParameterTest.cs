using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class ModelWithAnnotationUsedWithCunstructorParameterTest : Base.SqlTableDependencyBaseTest
    {
        [Table("AAWItemsTable")]
        private class ModelWithAnnotationUsedWithCunstructorParameterTestSqlServerModel
        {
            public long Id { get; set; }

            public string Name { get; set; }

            [Column("Long Description")]
            public string Infos { get; set; }
        }

        private static readonly string TableName = typeof(ModelWithAnnotationUsedWithCunstructorParameterTestSqlServerModel).Name;
        private static int _counter;
        private static readonly Dictionary<string, ModelWithAnnotationUsedWithCunstructorParameterTestSqlServerModel> CheckValues = new Dictionary<string, ModelWithAnnotationUsedWithCunstructorParameterTestSqlServerModel>();

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [int] IDENTITY(1, 1) NOT NULL, [Name] [NVARCHAR](50) NULL, [More Info] [NVARCHAR](MAX) NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize]
        public void TestInitialize()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new ModelWithAnnotationUsedWithCunstructorParameterTestSqlServerModel());
            CheckValues.Add(ChangeType.Update.ToString(), new ModelWithAnnotationUsedWithCunstructorParameterTestSqlServerModel());
            CheckValues.Add(ChangeType.Delete.ToString(), new ModelWithAnnotationUsedWithCunstructorParameterTestSqlServerModel());
        }

        [ClassCleanup]
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
        public void Test()
        {
            SqlTableDependency<ModelWithAnnotationUsedWithCunstructorParameterTestSqlServerModel> tableDependency = null;
            string naming = null;

            var mapper = new ModelToTableMapper<ModelWithAnnotationUsedWithCunstructorParameterTestSqlServerModel>();
            mapper.AddMapping(c => c.Infos, "More Info");

            var updateOf = new UpdateOfModel<ModelWithAnnotationUsedWithCunstructorParameterTestSqlServerModel>();
            updateOf.Add(i => i.Infos);

            try
            {
                tableDependency = new SqlTableDependency<ModelWithAnnotationUsedWithCunstructorParameterTestSqlServerModel>(ConnectionStringForTestUser, tableName: TableName, mapper: mapper, updateOf: updateOf);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;
                
                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Name, "Pizza MERGHERITA");
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Infos, "Pizza MERGHERITA");

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Name, "Pizza MERGHERITA");
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Infos, "FUNGHI PORCINI");

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Name, "Pizza");
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Infos, "FUNGHI PORCINI");

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming)== 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<ModelWithAnnotationUsedWithCunstructorParameterTestSqlServerModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _counter++;                    
                    CheckValues[ChangeType.Insert.ToString()].Name = e.Entity.Name;
                    CheckValues[ChangeType.Insert.ToString()].Infos = e.Entity.Infos;
                    break;

                case ChangeType.Update:
                    _counter++;
                    CheckValues[ChangeType.Update.ToString()].Name = e.Entity.Name;
                    CheckValues[ChangeType.Update.ToString()].Infos = e.Entity.Infos;
                    break;

                case ChangeType.Delete:
                    _counter++;
                    CheckValues[ChangeType.Delete.ToString()].Name = e.Entity.Name;
                    CheckValues[ChangeType.Delete.ToString()].Infos = e.Entity.Infos;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name], [More Info]) VALUES ('Pizza MERGHERITA', 'Pizza MERGHERITA')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [More Info] = 'FUNGHI PORCINI'";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = 'Pizza'";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}