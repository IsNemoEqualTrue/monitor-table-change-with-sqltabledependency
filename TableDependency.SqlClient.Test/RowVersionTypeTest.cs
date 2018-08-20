using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class RowVersionTypeTest : Base.SqlTableDependencyBaseTest
    {
        private class RowVersionTypeModel
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public byte[] Version { get; set; }
        }

        private static readonly string TableName = typeof(RowVersionTypeModel).Name;
        private byte[] _rowVersionInsert;
        private byte[] _rowVersionUpdate;
        private byte[] _rowVersionInsertOld;
        private byte[] _rowVersionUpdateOld;

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

                    sqlCommand.CommandText = $"CREATE TABLE {TableName}(Id INT, Name VARCHAR(50), Version ROWVERSION);";
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
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }

            _rowVersionInsert = null;
            _rowVersionUpdate = null;
            _rowVersionInsertOld = null;
            _rowVersionUpdateOld = null;
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
            SqlTableDependency<RowVersionTypeModel> tableDependency = null;

            try
            {
                tableDependency = new SqlTableDependency<RowVersionTypeModel>(ConnectionStringForTestUser);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.Start();

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreNotEqual(_rowVersionInsert, _rowVersionUpdate);
            Assert.IsNull(_rowVersionInsertOld);
            Assert.IsNull(_rowVersionUpdateOld);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TestWithOldValues()
        {
            SqlTableDependency<RowVersionTypeModel> tableDependency = null;

            try
            {
                tableDependency = new SqlTableDependency<RowVersionTypeModel>(ConnectionStringForTestUser, includeOldValues: true);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.Start();

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreNotEqual(_rowVersionInsert, _rowVersionUpdate);
            Assert.IsTrue(_rowVersionUpdateOld.SequenceEqual(_rowVersionInsert));
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<RowVersionTypeModel> e)
        {

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _rowVersionInsert = e.Entity.Version;
                    _rowVersionInsertOld = e.EntityOldValues?.Version;
                    break;

                case ChangeType.Update:
                    _rowVersionUpdate = e.Entity.Version;
                    _rowVersionUpdateOld = e.EntityOldValues?.Version;
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
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name]) VALUES (1, 'AA')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = 'BB' WHERE [Id] = 1";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}