using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.EventArgs;
using TableDependency.Mappers;

namespace TableDependency.SqlClient.IntegrationTest
{
    [TestClass]
    public class Issue_3
    {
        private static string _connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private const string TableName = "BiddingTextDependancy";

        public TestContext TestContext { get; set; }

        [TestInitialize]
        public void TestInitialize()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = "DROP TABLE BiddingTextDependancy";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = "CREATE TABLE BiddingTextDependancy ([id][int] IDENTITY(1, 1) NOT NULL, [full_text] [NVARCHAR](100) NOT NULL, [saleid] [NVARCHAR](100) NOT NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestMethod]
        public void ModelToTableMapper_only_seems_to_work_with_strings()
        {
            SqlTableDependency<BiddingTextDependancyDto> tableDependency = null;

            try
            {
                var mapper = new ModelToTableMapper<BiddingTextDependancyDto>();
                mapper.AddMapping(c => c.Id, "id").AddMapping(c => c.Text, "full_text").AddMapping(c => c.SaleId, "saleid");

                tableDependency = new SqlTableDependency<BiddingTextDependancyDto>(_connectionString, TableName, mapper);
                tableDependency.OnChanged += TableDependency_Changed;
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
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<BiddingTextDependancyDto> e)
        {
            this.TestContext.WriteLine(e.ChangeType + ": " + e.Entity.Id + " " + e.Entity.Text + " " + e.Entity.SaleId);
        }

        private static void ModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [dbo].[BiddingTextDependancy] ([full_text], [saleid]) VALUES ('BBBBBBBBBB', 987)";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = $"UPDATE [dbo].[BiddingTextDependancy] SET [full_text] = 'AAAAAAAAAAAA', [saleid] = 123";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = "DELETE FROM [dbo].[BiddingTextDependancy]";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);
                }
            }
        }

        public class BiddingTextDependancyDto
        {
            public long Id { get; set; }
            public string Text { get; set; }
            public int SaleId { get; set; }
        }
    }
}