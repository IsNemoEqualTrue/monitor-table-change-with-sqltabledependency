using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.SqlServer;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest.TypeChecks
{
    public class XmlNVarcharMaxAndVarcharMaxModel
    {
        // *****************************************************
        // SQL Server Data Type Mappings: 
        // https://msdn.microsoft.com/en-us/library/cc716729%28v=vs.110%29.aspx?f=255&MSPPError=-2147217396
        // *****************************************************
        public string varcharMAXColumn { get; set; }
        public string nvarcharMAXColumn { get; set; }
    }

    [TestClass]
    public class XmlNVarcharMaxAndVarcharMaxType
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServerConnectionString"].ConnectionString;
        private static string TableName = "Test";
        private static readonly Dictionary<string, Tuple<XmlNVarcharMaxAndVarcharMaxModel, XmlNVarcharMaxAndVarcharMaxModel>> CheckValues = new Dictionary<string, Tuple<XmlNVarcharMaxAndVarcharMaxModel, XmlNVarcharMaxAndVarcharMaxModel>>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"CREATE TABLE {TableName}(varcharMAXColumn VARCHAR(MAX) NULL, NvarcharMAXColumn NVARCHAR(MAX) NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize()]
        public void TestInitialize()
        {
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void ColumnTypesTest1()
        {
            SqlTableDependency<XmlNVarcharMaxAndVarcharMaxModel> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<XmlNVarcharMaxAndVarcharMaxModel>(ConnectionString, TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent1);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.varcharMAXColumn, CheckValues[ChangeType.Insert.ToString()].Item1.varcharMAXColumn);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.nvarcharMAXColumn, CheckValues[ChangeType.Insert.ToString()].Item1.nvarcharMAXColumn);
            
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.varcharMAXColumn, CheckValues[ChangeType.Update.ToString()].Item1.varcharMAXColumn);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.nvarcharMAXColumn, CheckValues[ChangeType.Update.ToString()].Item1.nvarcharMAXColumn);
            
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.varcharMAXColumn, CheckValues[ChangeType.Delete.ToString()].Item1.varcharMAXColumn);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.nvarcharMAXColumn, CheckValues[ChangeType.Delete.ToString()].Item1.nvarcharMAXColumn);
            
            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<XmlNVarcharMaxAndVarcharMaxModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.varcharMAXColumn = e.Entity.varcharMAXColumn;
                    CheckValues[ChangeType.Insert.ToString()].Item2.nvarcharMAXColumn = e.Entity.nvarcharMAXColumn;
                    break;
                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.varcharMAXColumn = e.Entity.varcharMAXColumn;
                    CheckValues[ChangeType.Update.ToString()].Item2.nvarcharMAXColumn = e.Entity.nvarcharMAXColumn;
                    break;
                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.varcharMAXColumn = e.Entity.varcharMAXColumn;
                    CheckValues[ChangeType.Delete.ToString()].Item2.nvarcharMAXColumn = e.Entity.nvarcharMAXColumn;
                    break;
            }
        }

        public static SqlXml ConvertString2SqlXml(string xmlData)
        {
            var encoding = new UTF8Encoding();
            var m = new MemoryStream(encoding.GetBytes(xmlData));
            return new SqlXml(m);
        }

        private static void ModifyTableContent1()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<XmlNVarcharMaxAndVarcharMaxModel, XmlNVarcharMaxAndVarcharMaxModel>(new XmlNVarcharMaxAndVarcharMaxModel { varcharMAXColumn = new string('*', 6000), nvarcharMAXColumn = new string('*', 8000) }, new XmlNVarcharMaxAndVarcharMaxModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<XmlNVarcharMaxAndVarcharMaxModel, XmlNVarcharMaxAndVarcharMaxModel>(new XmlNVarcharMaxAndVarcharMaxModel { varcharMAXColumn = "111", nvarcharMAXColumn = "new byte[] { 1, 2, 3, 4, 5, 6 }" }, new XmlNVarcharMaxAndVarcharMaxModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<XmlNVarcharMaxAndVarcharMaxModel, XmlNVarcharMaxAndVarcharMaxModel>(new XmlNVarcharMaxAndVarcharMaxModel { varcharMAXColumn = "111", nvarcharMAXColumn = "new byte[] { 1, 2, 3, 4, 5, 6 }" }, new XmlNVarcharMaxAndVarcharMaxModel()));

            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([varcharMAXColumn], [nvarcharMAXColumn]) VALUES(@varcharMAXColumn, @nvarcharMAXColumn)";
                    sqlCommand.Parameters.AddWithValue("@varcharMAXColumn", CheckValues[ChangeType.Insert.ToString()].Item1.varcharMAXColumn);
                    sqlCommand.Parameters.AddWithValue("@nvarcharMAXColumn", CheckValues[ChangeType.Insert.ToString()].Item1.nvarcharMAXColumn);
                    sqlCommand.ExecuteNonQuery();
                }

                Thread.Sleep(1000);

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [varcharMAXColumn] = @varcharMAXColumn, [nvarcharMAXColumn] = @nvarcharMAXColumn";
                    sqlCommand.Parameters.AddWithValue("@varcharMAXColumn", CheckValues[ChangeType.Update.ToString()].Item1.varcharMAXColumn);
                    sqlCommand.Parameters.AddWithValue("@nvarcharMAXColumn", CheckValues[ChangeType.Update.ToString()].Item1.nvarcharMAXColumn);
                    sqlCommand.ExecuteNonQuery();
                }

                Thread.Sleep(1000);

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }

                Thread.Sleep(1000);
            }
        }       
    }
}