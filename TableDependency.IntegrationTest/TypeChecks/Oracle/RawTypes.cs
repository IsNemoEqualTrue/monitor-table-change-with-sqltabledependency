using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.ManagedDataAccess.Client;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.OracleClient;

namespace TableDependency.IntegrationTest.TypeChecks.Oracle
{
    public class RAWTypeModel
    {
        public byte[] RAWCOLUMN { get; set; }
    }

    [TestClass]
    public class RAWTypeTest
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static readonly string TableName = "ARAWSTABLE";
        private static RAWTypeModel GotModel = new RAWTypeModel();
        private static RAWTypeModel SetModel = new RAWTypeModel();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            OracleHelper.DropTable(ConnectionString, TableName);

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {TableName}(RAWCOLUMN RAW(2000))";
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
        public void CheckTypeTest()
        {
            OracleTableDependency<RAWTypeModel> tableDependency = null;

            try
            {
                tableDependency = new OracleTableDependency<RAWTypeModel>(ConnectionString, TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.OnError += TableDependency_OnError;
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

            Assert.AreEqual(GetString(SetModel.RAWCOLUMN), GetString(GotModel.RAWCOLUMN));
        }

        private void TableDependency_OnError(object sender, ErrorEventArgs e)
        {
            throw e.Error;
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<RAWTypeModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    GotModel.RAWCOLUMN = e.Entity.RAWCOLUMN;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            SetModel.RAWCOLUMN = GetBytes("Nonna Dirce");

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    var parameters = new[] {new OracleParameter() { ParameterName = "rawvalue", OracleDbType = OracleDbType.Raw, Value = SetModel.RAWCOLUMN } };
                    command.CommandText = $"BEGIN INSERT INTO {TableName}(RAWCOLUMN) VALUES (:rawvalue); END;";
                    command.Parameters.AddRange(parameters);
                    command.ExecuteNonQuery();
                }

                Thread.Sleep(5000);
            }
        }

        static byte[] GetBytes(string str)
        {
            if (str == null) return null;
            byte[] bytes = new byte[str.Length * sizeof(char)];
            System.Buffer.BlockCopy(str.ToCharArray(), 0, bytes, 0, bytes.Length);
            return bytes;
        }

        static string GetString(byte[] bytes)
        {
            if (bytes == null) return null;
            char[] chars = new char[bytes.Length / sizeof(char)];
            System.Buffer.BlockCopy(bytes, 0, chars, 0, bytes.Length);
            return new string(chars);
        }
    }
}