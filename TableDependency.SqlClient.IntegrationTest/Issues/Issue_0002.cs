using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.EventArgs;
using TableDependency.SqlClient.IntegrationTest.Helpers;

namespace TableDependency.SqlClient.IntegrationTest.Issues
{
    [TestClass]
    public class Issue_0002
    {
        private static string _connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private const string TableName = "Issue0002";
        private int _counter;

        /// <summary>
        /// Gets or sets the test context which provides information about and functionality for the current test run.
        /// </summary>
        /// <value>
        /// The test context.
        /// </value>
        public TestContext TestContext { get; set; }

        [TestInitialize]
        public void TestInitialize()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText =
                        $"IF OBJECT_ID('{TableName}', 'U') IS NULL BEGIN CREATE TABLE [{TableName}]( " +
                        "[Id][int] IDENTITY(1, 1) NOT NULL," +
                        "[VarcharColumn] [nvarchar](4000) NULL," +
                        "[DateTime2Column] [datetime2](7) NULL," +
                        "[DatetimeOffsetColumn] [datetimeoffset](7) NULL," +
                        "[TimeColumn] [time](7) NULL," +
                        "[TimeStampColumn] [timestamp] NULL) END;";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestMethod]
        public void ColumnsSizeTest()
        {
            SqlTableDependency<Model.Issue_0004_Model> tableDependency = null;
            string naming = null;

            try
            {
                tableDependency = new SqlTableDependency<Model.Issue_0004_Model>(_connectionString, TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
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

            Assert.IsTrue(_counter == 3);
            Assert.IsTrue(Helper.AreAllDbObjectDisposed(_connectionString, naming));
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<Model.Issue_0004_Model> e)
        {
            _counter++;
            this.TestContext.WriteLine($"{e.ChangeType}: {e.Entity.VarcharColumn}");
        }

        private static void ModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([VarcharColumn]) VALUES ('La pizza Margherita è una tipica pizza napoletana condita con pomodoro, mozzarella, basilico fresco, sale ed olio. La mozzarella, nella pizza Margherita tradizionale, non è quella di bufala, ma il fior di latte. È, assieme alla pizza marinara, la più popolare pizza napoletana.')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [VarcharColumn] = 'MARGHERITA'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);
                }
            }
        }
    }
}