using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.DataAccess.Client;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.Mappers;
using TableDependency.OracleClient;

namespace TableDependency.IntegrationTest.TypeChecks.Oracle
{
    public class Varchar2Model
    {
        public string Name { get; set; }
    }

    [TestClass]
    public class Varchar2Type
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static readonly string TableName = "AAATEST";
        private static string _name;

        public TestContext TestContext { get; set; }

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            OracleHelper.DropTable(ConnectionString, TableName);

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {TableName}(COLUMN3 VARCHAR2(4000), XMLCOLUMN XMLTYPE)";
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
        public void CheckTest()
        {
            OracleTableDependency<Varchar2Model> tableDependency = null;

            try
            {
                var mapper = new ModelToTableMapper<Varchar2Model>();
                mapper.AddMapping(c => c.Name, "COLUMN3");

                tableDependency = new OracleTableDependency<Varchar2Model>(ConnectionString, TableName, mapper);
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

            Assert.AreEqual(new string('*', 4000), _name);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<Varchar2Model> e)
        {
            _name = e.Entity.Name;
        }

        private static void ModifyTableContent()
        {
            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"BEGIN INSERT INTO {TableName}(COLUMN3) VALUES ('" + new string('*', 4000) + "'); END;";
                    command.ExecuteNonQuery();
                }

                Thread.Sleep(5000);
            }
        }
    }
}