using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Base;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class Issue146Test : Base.SqlTableDependencyBaseTest
    {
        #region Model definitions

        public class DomainObject 
        {
            public int Id { get; set; }
            public string Name { get; set; }
        }

        public partial class Bank : DomainObject
        {
            public string Description { get; set; }
        }

        #endregion

        private static readonly string TableName = "Bank";
        private static readonly Dictionary<string, List<Bank>> CheckValues = new Dictionary<string, List<Bank>>();

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('[{TableName}]', 'U') IS NOT NULL DROP TABLE [dbo].[{TableName}]";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}] ([IdBank] [INT] NOT NULL PRIMARY KEY, [BankName] NVARCHAR(50), [BankDescription] NVARCHAR(100))";
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

            Issue146Test.CheckValues.Clear();

            Issue146Test.CheckValues.Add(ChangeType.Insert.ToString(), new List<Bank>());
            Issue146Test.CheckValues.Add(ChangeType.Update.ToString(), new List<Bank>());
            Issue146Test.CheckValues.Add(ChangeType.Delete.ToString(), new List<Bank>());
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
        public void Test1()
        {
            SqlTableDependency<Bank> tableDependency = null;
            string naming;

            try
            {
                var mapper = new ModelToTableMapper<Bank>();
                mapper.AddMapping(c => c.Id, "IdBank"); // dbo,Bank.IdBank (PK, int, not null)
                mapper.AddMapping(c => c.Name, "BankName");
                mapper.AddMapping(c => c.Description, "BankDescription");

                tableDependency = new SqlTableDependency<Bank>(ConnectionStringForTestUser, TableName, mapper: mapper);
                tableDependency.OnChanged += this.TableDependency_Changed1;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent1);
                t.Start();
                Thread.Sleep(1000 * 5 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(Issue146Test.CheckValues[ChangeType.Insert.ToString()][0].Id, 1);
            Assert.AreEqual(Issue146Test.CheckValues[ChangeType.Insert.ToString()][0].Name, "UBS bank");
            Assert.AreEqual(Issue146Test.CheckValues[ChangeType.Insert.ToString()][0].Description, "THE UBS bank");

            Assert.AreEqual(Issue146Test.CheckValues[ChangeType.Update.ToString()][0].Id, 2);
            Assert.AreEqual(Issue146Test.CheckValues[ChangeType.Update.ToString()][0].Name, "Credit Swiss Bank");
            Assert.AreEqual(Issue146Test.CheckValues[ChangeType.Update.ToString()][0].Description, "THE Credit Swiss Bank");

            Assert.AreEqual(Issue146Test.CheckValues[ChangeType.Delete.ToString()][0].Id, 2);
            Assert.AreEqual(Issue146Test.CheckValues[ChangeType.Delete.ToString()][0].Name, "Credit Swiss Bank");
            Assert.AreEqual(Issue146Test.CheckValues[ChangeType.Delete.ToString()][0].Description, "THE Credit Swiss Bank");

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private void TableDependency_Changed1(object sender, RecordChangedEventArgs<Bank> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    Issue146Test.CheckValues[ChangeType.Insert.ToString()].Add(e.Entity);
                    break;

                case ChangeType.Update:
                    Issue146Test.CheckValues[ChangeType.Update.ToString()].Add(e.Entity);
                    break;

                case ChangeType.Delete:
                    Issue146Test.CheckValues[ChangeType.Delete.ToString()].Add(e.Entity);
                    break;
            }
        }

        private static void ModifyTableContent1()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([IdBank], [BankName], [BankDescription]) VALUES(1, 'UBS bank', 'THE UBS bank')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [IdBank] = 2, [BankName] = 'Credit Swiss Bank', [BankDescription] = 'THE Credit Swiss Bank' WHERE [IdBank] = 1";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}] WHERE [IdBank] = 2";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}