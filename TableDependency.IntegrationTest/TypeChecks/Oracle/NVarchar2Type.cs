using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.DataAccess.Client;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.OracleClient;

namespace TableDependency.IntegrationTest.TypeChecks.Oracle
{
    public class NVarchar2CharModel
    {
        public string NVARCHARCOLUMN { get; set; }
    }

    [TestClass]
    public class NVarchar2Type
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static readonly string TableName = "ANVARCHARTABLE";
        private static string STRING_TEST_1 = "Désolé";
        private static string STRING_TEST_2 = new string('Ü', 4000);
        private static string STRING_TEST_3 = "这里输要读的文字或";
        private static string _gotString;

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            OracleHelper.DropTable(ConnectionString, TableName);

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {TableName}(NVARCHARCOLUMN NVARCHAR2(4000))";
                    command.ExecuteNonQuery();
                }
            }
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            OracleHelper.DropTable(ConnectionString, TableName);
        }

        [TestMethod]
        public void CheckTypeTest1()
        {
            OracleTableDependency<NVarchar2CharModel> tableDependency = null;

            try
            {
                tableDependency = new OracleTableDependency<NVarchar2CharModel>(ConnectionString, TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.OnError += TableDependency_OnError;
                tableDependency.Start();
                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent1);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_gotString, STRING_TEST_1);
        }

        [TestMethod]
        public void CheckTypeTest2()
        {
            OracleTableDependency<NVarchar2CharModel> tableDependency = null;

            try
            {
                tableDependency = new OracleTableDependency<NVarchar2CharModel>(ConnectionString, TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.OnError += TableDependency_OnError;
                tableDependency.Start();
                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent2);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_gotString, STRING_TEST_2);
        }

        [TestMethod]
        public void CheckTypeTest3()
        {
            OracleTableDependency<NVarchar2CharModel> tableDependency = null;

            try
            {
                tableDependency = new OracleTableDependency<NVarchar2CharModel>(ConnectionString, TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.OnError += TableDependency_OnError;      
                tableDependency.Start();
                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent3);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_gotString, STRING_TEST_3);
        }

        private void TableDependency_OnError(object sender, ErrorEventArgs e)
        {
            throw e.Error;
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<NVarchar2CharModel> e)
        {
            _gotString = e.Entity.NVARCHARCOLUMN;
        }

        private static void ModifyTableContent1()
        {
            ModifyTableContent(STRING_TEST_1);
        }

        private static void ModifyTableContent2()
        {
            ModifyTableContent(STRING_TEST_2);
        }

        private static void ModifyTableContent3()
        {
            ModifyTableContent(STRING_TEST_3);
        }

        private static void ModifyTableContent(string p)
        {
            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"BEGIN INSERT INTO {TableName}(NVARCHARCOLUMN) VALUES (:v3); END;";
                    command.Parameters.Add(new OracleParameter("v3", OracleDbType.NVarchar2) { Value = p });
                    command.ExecuteNonQuery();
                }

                Thread.Sleep(1000);
            }
        }
    }
}