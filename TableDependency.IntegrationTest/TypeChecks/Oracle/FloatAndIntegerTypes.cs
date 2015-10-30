using System;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Oracle.DataAccess.Client;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.Oracle;
using TableDependency.OracleClient;

namespace TableDependency.IntegrationTest.TypeChecks.Oracle
{
    public class NumberTypesModel
    {
        public decimal FLOATCOLUMN { get; set; }
        public decimal INTEGERCOLUMN { get; set; }
    }

    [TestClass]
    public class FloatAndIntegerTypes
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
        private static readonly string TableName = "NUMBERS";
        private static NumberTypesModel GotNumberTypesModel = new NumberTypesModel();
        private static NumberTypesModel SetNumberTypesModel = new NumberTypesModel();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            OracleHelper.DropTable(ConnectionString, TableName);

            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"CREATE TABLE {TableName}(FLOATCOLUMN FLOAT,INTEGERCOLUMN INTEGER)";
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
        public void CheckDateTypeTest()
        {
            OracleTableDependency<NumberTypesModel> tableDependency = null;

            try
            {
                tableDependency = new OracleTableDependency<NumberTypesModel>(ConnectionString, TableName);
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

            Assert.AreEqual(GotNumberTypesModel.FLOATCOLUMN, SetNumberTypesModel.FLOATCOLUMN);
            Assert.AreEqual(GotNumberTypesModel.FLOATCOLUMN, SetNumberTypesModel.FLOATCOLUMN);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<NumberTypesModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    GotNumberTypesModel.FLOATCOLUMN = e.Entity.FLOATCOLUMN;
                    GotNumberTypesModel.INTEGERCOLUMN = e.Entity.INTEGERCOLUMN;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            SetNumberTypesModel.FLOATCOLUMN = 12.3M;
            SetNumberTypesModel.INTEGERCOLUMN = 34.1M;


            using (var connection = new OracleConnection(ConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    var parameters = new[] {
                        new OracleParameter("FLOATCOLUMNvar", SetNumberTypesModel.FLOATCOLUMN),
                        new OracleParameter("INTEGERCOLUMNvar", SetNumberTypesModel.INTEGERCOLUMN)};

                    command.CommandText = $"BEGIN INSERT INTO {TableName}(FLOATCOLUMN,INTEGERCOLUMN) VALUES (:FLOATCOLUMNvar, :INTEGERCOLUMNvar); END;";
                    command.Parameters.AddRange(parameters);
                    command.ExecuteNonQuery();
                }

                Thread.Sleep(1000);
            }
        }
    }
}