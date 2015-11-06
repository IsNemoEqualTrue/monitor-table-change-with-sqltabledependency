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
    //public class NVarchar2CharModel
    //{
    //    public string VARCHARCOLUMN { get; set; }
    //    public string NVARCHARCOLUMN { get; set; }
    //    //public char[] CHARCOLUMN { get; set; }
    //    //public char[] NCHARCOLUMN { get; set; }
    //}

    //[TestClass]
    //public class NVarchar2Type
    //{
    //    private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["OracleConnectionString"].ConnectionString;
    //    private static readonly string TableName = "ACHATTABLE";
    //    private static NVarchar2CharModel GotModel = new NVarchar2CharModel();
    //    private static NVarchar2CharModel SetModel = new NVarchar2CharModel();

    //    [ClassInitialize()]
    //    public static void ClassInitialize(TestContext testContext)
    //    {
    //        OracleHelper.DropTable(ConnectionString, TableName);

    //        using (var connection = new OracleConnection(ConnectionString))
    //        {
    //            connection.Open();
    //            using (var command = connection.CreateCommand())
    //            {
    //                // NCHARCOLUMN NCHAR(100),CHARCOLUMN CHAR(100),
    //                command.CommandText = $"CREATE TABLE {TableName}(NVARCHARCOLUMN NVARCHAR2(100),VARCHARCOLUMN VARCHAR2(100))";
    //                command.ExecuteNonQuery();
    //            }
    //        }
    //    }

    //    [ClassCleanup()]
    //    public static void ClassCleanup()
    //    {
    //        OracleHelper.DropTable(ConnectionString, TableName);
    //    }

    //    [TestMethod]
    //    public void CheckTypeTest()
    //    {
    //        OracleTableDependency<NVarchar2CharModel> tableDependency = null;

    //        try
    //        {
    //            tableDependency = new OracleTableDependency<NVarchar2CharModel>(ConnectionString, TableName);
    //            tableDependency.OnChanged += this.TableDependency_Changed;
    //            tableDependency.OnError += TableDependency_OnError;      
    //            tableDependency.Start();
    //            Thread.Sleep(5000);

    //            var t = new Task(ModifyTableContent);
    //            t.Start();
    //            t.Wait(20000);
    //        }
    //        finally
    //        {
    //            tableDependency?.Dispose();
    //        }

    //        Assert.AreEqual(GotModel.NVARCHARCOLUMN, SetModel.NVARCHARCOLUMN);
    //        Assert.AreEqual(GotModel.VARCHARCOLUMN, SetModel.VARCHARCOLUMN);
    //        //Assert.AreEqual(GotModel.NCHARCOLUMN, SetModel.NCHARCOLUMN);
    //        //Assert.AreEqual(GotModel.CHARCOLUMN, SetModel.CHARCOLUMN);

    //    }

    //    private void TableDependency_OnError(object sender, ErrorEventArgs e)
    //    {
    //        throw e.Error;
    //    }

    //    private void TableDependency_Changed(object sender, RecordChangedEventArgs<NVarchar2CharModel> e)
    //    {
    //        switch (e.ChangeType)
    //        {
    //            case ChangeType.Insert:
    //                //GotModel.CHARCOLUMN = e.Entity.CHARCOLUMN;
    //                //GotModel.NCHARCOLUMN = e.Entity.NCHARCOLUMN;
    //                GotModel.VARCHARCOLUMN = e.Entity.VARCHARCOLUMN;
    //                GotModel.NVARCHARCOLUMN = e.Entity.NVARCHARCOLUMN;
    //                break;
    //        }
    //    }

    //    private static void ModifyTableContent()
    //    {
    //        //SetModel.CHARCOLUMN = "Spiacente".ToCharArray();
    //        //SetModel.NCHARCOLUMN = "Désolé".ToCharArray();
    //        SetModel.VARCHARCOLUMN = "Spiacente";
    //        SetModel.NVARCHARCOLUMN = "Désolé"; // 这里输要读的文字或

    //        using (var connection = new OracleConnection(ConnectionString))
    //        {
    //            connection.Open();

    //            using (var command = connection.CreateCommand())
    //            {
    //                // NCHARCOLUMN, CHARCOLUMN, :v1, :v2, 
    //                command.CommandText = $"BEGIN INSERT INTO {TableName}(NVARCHARCOLUMN, VARCHARCOLUMN) VALUES (:v3, :v4); END;";
    //                //command.Parameters.Add(new OracleParameter("v1", OracleDbType.NChar) { Value = SetModel.NCHARCOLUMN });
    //                //command.Parameters.Add(new OracleParameter("v2", OracleDbType.Char) { Value = SetModel.CHARCOLUMN });
    //                command.Parameters.Add(new OracleParameter("v3", OracleDbType.NVarchar2) { Value = SetModel.NVARCHARCOLUMN });
    //                command.Parameters.Add(new OracleParameter("v4", OracleDbType.Varchar2) { Value = SetModel.VARCHARCOLUMN });
    //                command.ExecuteNonQuery();
    //            }

    //            Thread.Sleep(1000);
    //        }
    //    }
    //}
}