using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest.TypeChecks.SqlServer
{
    public class RowVersioneModel
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public byte[] Version { get; set; }

}

    [TestClass]
    public class RowVersionType
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServerConnectionString"].ConnectionString;
        private static string TableName = "RowVersionTest";
        private byte[] RowVersionInsert = null;
        private byte[] RowVersionUpdate = null;

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

                    sqlCommand.CommandText = $"CREATE TABLE {TableName}(Id INT, Name VARCHAR(50), Version ROWVERSION);";
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
        public void Test()
        {
            SqlTableDependency<RowVersioneModel> tableDependency = null;

            try
            {
                tableDependency = new SqlTableDependency<RowVersioneModel>(ConnectionString, TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.Start();

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreNotEqual(RowVersionInsert, RowVersionUpdate);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<RowVersioneModel> e)
        {

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    RowVersionInsert = e.Entity.Version;
                    break;

                case ChangeType.Update:
                    RowVersionUpdate = e.Entity.Version;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name]) VALUES (1, 'AA')";                   
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);
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