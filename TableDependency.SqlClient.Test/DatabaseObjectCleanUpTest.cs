namespace TableDependency.SqlClient.Test
{
#if DEBUG
    [TestClass]
    public class DatabaseObjectCleanUpTest : SqlTableDependencyBaseTest
    {
        private class DatabaseObjectCleanUpSqlServerModel
        {
            public int Id { get; set; }
            public string Description { get; set; }
        }

        private static readonly string TableName = typeof(DatabaseObjectCleanUpSqlServerModel).Name;

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

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id][int], [First Name] [nvarchar](50), [Second Name] [nvarchar](50))";
                    sqlCommand.ExecuteNonQuery();
                }
            }
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
            var tableDependency = new SqlTableDependency<DatabaseObjectCleanUpSqlServerModel>(ConnectionStringForTestUser, tableName: TableName);
            tableDependency.OnChanged += TableDependency_OnChanged;
            tableDependency.Start();
            var dbObjectsNaming = tableDependency.DataBaseObjectsNamingConvention;

            Thread.Sleep(10000);
            
            tableDependency.StopWithoutDisposing();

            Thread.Sleep(4 * 60 * 1000);
            Assert.IsTrue(base.AreAllDbObjectDisposed(dbObjectsNaming));
            Assert.IsTrue(base.CountConversationEndpoints(dbObjectsNaming) == 0);
        }

        private void TableDependency_OnChanged(object sender, TableDependency.EventArgs.RecordChangedEventArgs<DatabaseObjectCleanUpSqlServerModel> e)
        {
        }
    }
#endif
}