using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.Mappers;
using TableDependency.OracleClient;

namespace TableDependency.IntegrationTest.TypeChecks.Oracle
{
    public class XmlAModel
    {
        public string XmlColumn { get; set; }
    }

    [TestClass]
    public class XmlAType
    {
        private static string XML = "<names><name>Adélaïde</name><name>这里输要读的文字或</name><name>Згинуть наші воріженьки, як роса на сонці</name><name>أبوس الأرض تحت نعالكم</name><name>" + new string('*', 4000) + "</name></names>";
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static readonly string TableName = "AAAXMLTEST";
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

        [TestCategory("Oracle")]
        [TestMethod]
        public void CheckTest()
        {
            OracleTableDependency<XmlAModel> tableDependency = null;

            try
            {
                var mapper = new ModelToTableMapper<XmlAModel>();
                mapper.AddMapping(c => c.XmlColumn, "XMLCOLUMN");

                tableDependency = new OracleTableDependency<XmlAModel>(ConnectionString, TableName, mapper);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.Start();
                Thread.Sleep(1000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(2000);
            }
            finally
            {
                tableDependency?.Dispose();
            }


            var expectedXml = new XmlDocument();
            expectedXml.LoadXml(XML);
            var gotXml = new XmlDocument();
            gotXml.LoadXml(_xml);

            Assert.AreEqual(gotXml.InnerXml, expectedXml.InnerXml);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<XmlAModel> e)
        {
            _xml = e.Entity.XmlColumn;
        }

        private static void ModifyTableContent()
        {
            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"BEGIN INSERT INTO {TableName}(XMLCOLUMN) VALUES (:ProductInfo); END;";

                    var productInfoParam = new OracleParameter("ProductInfo", OracleDbType.XmlType);
                    productInfoParam.Value = new OracleXmlType(connection, XML); 
                    command.Parameters.Add(productInfoParam);

                    command.ExecuteNonQuery();
                }

                Thread.Sleep(5000);
            }
        }
    }
}