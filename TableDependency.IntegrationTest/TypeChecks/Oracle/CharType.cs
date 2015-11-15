using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.OracleClient;
using Oracle.ManagedDataAccess.Client;

namespace TableDependency.IntegrationTest.TypeChecks.Oracle
{
    public class CharModel
    {
        public char[] CHARCOLUMN { get; set; }
        public char[] NCHARCOLUMN { get; set; }
    }
    
    [TestClass]
    public class CharType
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static readonly string TableName = "ANCHATTABLE";
        private static CharModel GotModel = new CharModel();
        private static CharModel SetModel = new CharModel();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            OracleHelper.DropTable(ConnectionString, TableName);

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {TableName}(NCHARCOLUMN NCHAR(1000),CHARCOLUMN CHAR(2000))";
                    command.ExecuteNonQuery();
                }
            }
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            OracleHelper.DropTable(ConnectionString, TableName);
        }

        [TestCategory("Oracle")]
        [TestMethod]
        public void CheckTypeTest1()
        {
            OracleTableDependency<CharModel> tableDependency = null;

            try
            {
                tableDependency = new OracleTableDependency<CharModel>(ConnectionString, TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.OnError += TableDependency_OnError;
                tableDependency.Start();
                Thread.Sleep(1000);

                var t = new Task(ModifyTableContent1);
                t.Start();
                t.Wait(2000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(new string(GotModel.CHARCOLUMN).Trim(), new string(SetModel.CHARCOLUMN).Trim());
            Assert.AreEqual(new string(GotModel.NCHARCOLUMN).Trim(), new string(SetModel.NCHARCOLUMN).Trim());            
        }

        [TestCategory("Oracle")]
        [TestMethod]
        public void CheckTypeTest2()
        {
            OracleTableDependency<CharModel> tableDependency = null;

            try
            {
                tableDependency = new OracleTableDependency<CharModel>(ConnectionString, TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.OnError += TableDependency_OnError;
                tableDependency.Start();
                Thread.Sleep(1000);

                var t = new Task(ModifyTableContent2);
                t.Start();
                t.Wait(2000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(new string(GotModel.CHARCOLUMN).Trim(), new string(SetModel.CHARCOLUMN).Trim());
            Assert.AreEqual(new string(GotModel.NCHARCOLUMN).Trim(), new string(SetModel.NCHARCOLUMN).Trim());            
        }

        private void TableDependency_OnError(object sender, ErrorEventArgs e)
        {
            throw e.Error;
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<CharModel> e)
        {
            GotModel.CHARCOLUMN = e.Entity.CHARCOLUMN;
            GotModel.NCHARCOLUMN = e.Entity.NCHARCOLUMN;
        }

        private static void ModifyTableContent1()
        {
            SetModel.CHARCOLUMN = "Spiacente".ToCharArray();
            SetModel.NCHARCOLUMN = "这里输要读的文字或".ToCharArray();

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"BEGIN INSERT INTO {TableName}(CHARCOLUMN,NCHARCOLUMN) VALUES (:v3, :v4); END;";
                    command.Parameters.Add(new OracleParameter("v3", OracleDbType.Char) { Value = SetModel.CHARCOLUMN });
                    command.Parameters.Add(new OracleParameter("v4", OracleDbType.NChar) { Value = SetModel.NCHARCOLUMN });
                    command.ExecuteNonQuery();
                }

                Thread.Sleep(1000);
            }
        }

        private static void ModifyTableContent2()
        {
            SetModel.CHARCOLUMN = new string('À', 1000).ToCharArray();
            SetModel.NCHARCOLUMN = new string('里', 500).ToCharArray();

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"BEGIN INSERT INTO {TableName}(CHARCOLUMN,NCHARCOLUMN) VALUES (:v3, :v4); END;";
                    command.Parameters.Add(new OracleParameter("v3", OracleDbType.Char, SetModel.CHARCOLUMN.Length) { Value = SetModel.CHARCOLUMN });
                    command.Parameters.Add(new OracleParameter("v4", OracleDbType.NChar, SetModel.NCHARCOLUMN.Length) { Value = SetModel.NCHARCOLUMN });
                    command.ExecuteNonQuery();
                }

                Thread.Sleep(1000);
            }
        }
    }
}