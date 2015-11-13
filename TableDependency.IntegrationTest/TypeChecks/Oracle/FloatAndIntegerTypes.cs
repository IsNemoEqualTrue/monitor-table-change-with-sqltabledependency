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
    public class FloatAndIntegerTypesModel
    {
        public decimal FLOATCOLUMN { get; set; }
        public decimal INTEGERCOLUMN { get; set; }
        public decimal NUMBERCOLUMN { get; set; }
    }

    [TestClass]
    public class FloatAndIntegerTypes
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static readonly string TableName = "AFLOATANDINTEGERTYPES";
        private static FloatAndIntegerTypesModel GotModel = new FloatAndIntegerTypesModel();
        private static FloatAndIntegerTypesModel SetModel = new FloatAndIntegerTypesModel();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            OracleHelper.DropTable(ConnectionString, TableName);

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {TableName}(FLOATCOLUMN FLOAT,INTEGERCOLUMN INTEGER, NUMBERCOLUMN NUMBER(9, 1))";
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
        public void CheckTypeTest()
        {
            OracleTableDependency<FloatAndIntegerTypesModel> tableDependency = null;

            try
            {
                tableDependency = new OracleTableDependency<FloatAndIntegerTypesModel>(ConnectionString, TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.OnError += this.TableDependency_OnError;
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

            Assert.AreEqual(GotModel.FLOATCOLUMN, SetModel.FLOATCOLUMN);
            Assert.AreEqual(GotModel.FLOATCOLUMN, SetModel.FLOATCOLUMN);
            Assert.AreEqual(GotModel.NUMBERCOLUMN, SetModel.NUMBERCOLUMN);            
        }

        private void TableDependency_OnError(object sender, ErrorEventArgs e)
        {
            throw e.Error;
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<FloatAndIntegerTypesModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    GotModel.FLOATCOLUMN = e.Entity.FLOATCOLUMN;
                    GotModel.INTEGERCOLUMN = e.Entity.INTEGERCOLUMN;
                    GotModel.NUMBERCOLUMN = e.Entity.NUMBERCOLUMN;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            SetModel.FLOATCOLUMN = 12.3M;
            SetModel.INTEGERCOLUMN = 34.1M;
            SetModel.INTEGERCOLUMN = 4751132.7M;

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    var parameters = new[] {
                        new OracleParameter("FLOATCOLUMNvar", SetModel.FLOATCOLUMN),
                        new OracleParameter("INTEGERCOLUMNvar", SetModel.INTEGERCOLUMN),
                        new OracleParameter("NUMBERCOLUMNvar", SetModel.NUMBERCOLUMN)};
                    
                    command.CommandText = $"BEGIN INSERT INTO {TableName}(FLOATCOLUMN,INTEGERCOLUMN,NUMBERCOLUMN) VALUES (:FLOATCOLUMNvar, :INTEGERCOLUMNvar, :NUMBERCOLUMNvar); END;";
                    command.Parameters.AddRange(parameters);
                    command.ExecuteNonQuery();
                }

                Thread.Sleep(5000);
            }
        }
    }
}