using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.DataAccess.Client;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.Mappers;
using TableDependency.OracleClient;

namespace TableDependency.IntegrationTest.TypeChecks.Oracle
{
    public class XmlAndVarchar2Model
    {
        public string XmlColumn { get; set; }
        public string Name { get; set; }
    }

    [TestClass]
    public class XmlAndVarchar2Type
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static readonly string TableName = "AAATESTS";
        private static string _name;
        private string _xml;

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
            OracleTableDependency<XmlAndVarchar2Model> tableDependency = null;

            try
            {
                var mapper = new ModelToTableMapper<XmlAndVarchar2Model>();
                mapper.AddMapping(c => c.Name, "COLUMN3");
                mapper.AddMapping(c => c.XmlColumn, "XMLCOLUMN");

                tableDependency = new OracleTableDependency<XmlAndVarchar2Model>(ConnectionString, TableName, mapper);
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


            var expectedXml = new XmlDocument();
            expectedXml.LoadXml("<names><name>Velia</name><name>Alfredina</name><name>Luciano</name></names>");
            var gotXml = new XmlDocument();
            gotXml.LoadXml(_xml);

            Assert.AreEqual(new string('*', 4000), _name);
            Assert.AreEqual(gotXml.InnerXml, expectedXml.InnerXml);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<XmlAndVarchar2Model> e)
        {
            _name = e.Entity.Name;
            _xml = e.Entity.XmlColumn;
        }

        private static void ModifyTableContent()
        {
            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = 
                        $"BEGIN INSERT INTO {TableName}(COLUMN3, XMLCOLUMN) VALUES ('" + 
                        new string('*', 4000) + "'," +
                        "XMLType('<names><name>Velia</name><name>Alfredina</name><name>Luciano</name></names>')); END;";

                    command.ExecuteNonQuery();
                }

                Thread.Sleep(5000);
            }
        }
    }
}