using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Helpers.SqlServer;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    public class XmlAsVarcharMaxSqlServerTestModell
    {
        // *****************************************************
        // SQL Server Data Type Mappings: 
        // https://msdn.microsoft.com/en-us/library/cc716729%28v=vs.110%29.aspx?f=255&MSPPError=-2147217396
        // *****************************************************
        public string VarcharMaxColumn { get; set; }
        public string NvarcharMaxColumn { get; set; }
    }

    [TestClass]
    public class XmlAsStringSqlServerTest
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServer2008 Test_User"].ConnectionString;
        private const string TableName = "XmlAsVarcharMaxSqlServerTestModell";
        private static readonly Dictionary<string, Tuple<XmlAsVarcharMaxSqlServerTestModell, XmlAsVarcharMaxSqlServerTestModell>> CheckValues = new Dictionary<string, Tuple<XmlAsVarcharMaxSqlServerTestModell, XmlAsVarcharMaxSqlServerTestModell>>();

        private const string XmlForInsert = @"<?xml version=""1.0"" encoding=""utf-8""?><catalog><book id=""1""><author>Gambardella, Matthew</author><title>XML Developer's Guide</title></book></catalog>";
        private const string XmlForUpdate = @"<?xml version=""1.0"" encoding=""utf-8""?><catalog><book id=""2""><author>Ridley, Matthew</author><title>XML Developer's Guide</title></book></catalog>";

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

                    sqlCommand.CommandText = $"CREATE TABLE {TableName}(VarcharMAXColumn VARCHAR(MAX) NULL, NvarcharMAXColumn NVARCHAR(MAX) NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
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
        public void XmlAsStringTest()
        {
            SqlTableDependency<XmlAsVarcharMaxSqlServerTestModell> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<XmlAsVarcharMaxSqlServerTestModell>(ConnectionString, TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent1);
                t.Start();
                t.Wait(1000 * 10 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.VarcharMaxColumn, CheckValues[ChangeType.Insert.ToString()].Item1.VarcharMaxColumn);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.NvarcharMaxColumn, CheckValues[ChangeType.Insert.ToString()].Item1.NvarcharMaxColumn);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.VarcharMaxColumn, CheckValues[ChangeType.Update.ToString()].Item1.VarcharMaxColumn);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.NvarcharMaxColumn, CheckValues[ChangeType.Update.ToString()].Item1.NvarcharMaxColumn);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.VarcharMaxColumn, CheckValues[ChangeType.Delete.ToString()].Item1.VarcharMaxColumn);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.NvarcharMaxColumn, CheckValues[ChangeType.Delete.ToString()].Item1.NvarcharMaxColumn);

            Thread.Sleep(1000 * 10 * 1);

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(SqlServerHelper.AreAllEndpointDisposed(naming));
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<XmlAsVarcharMaxSqlServerTestModell> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.VarcharMaxColumn = e.Entity.VarcharMaxColumn;
                    CheckValues[ChangeType.Insert.ToString()].Item2.NvarcharMaxColumn = e.Entity.NvarcharMaxColumn;
                    break;

                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.VarcharMaxColumn = e.Entity.VarcharMaxColumn;
                    CheckValues[ChangeType.Update.ToString()].Item2.NvarcharMaxColumn = e.Entity.NvarcharMaxColumn;
                    break;

                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.VarcharMaxColumn = e.Entity.VarcharMaxColumn;
                    CheckValues[ChangeType.Delete.ToString()].Item2.NvarcharMaxColumn = e.Entity.NvarcharMaxColumn;
                    break;
            }
        }

        private static void ModifyTableContent1()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<XmlAsVarcharMaxSqlServerTestModell, XmlAsVarcharMaxSqlServerTestModell>(new XmlAsVarcharMaxSqlServerTestModell { VarcharMaxColumn = XmlForInsert, NvarcharMaxColumn = XmlForInsert }, new XmlAsVarcharMaxSqlServerTestModell()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<XmlAsVarcharMaxSqlServerTestModell, XmlAsVarcharMaxSqlServerTestModell>(new XmlAsVarcharMaxSqlServerTestModell { VarcharMaxColumn = XmlForUpdate, NvarcharMaxColumn = XmlForUpdate }, new XmlAsVarcharMaxSqlServerTestModell()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<XmlAsVarcharMaxSqlServerTestModell, XmlAsVarcharMaxSqlServerTestModell>(new XmlAsVarcharMaxSqlServerTestModell { VarcharMaxColumn = XmlForUpdate, NvarcharMaxColumn = XmlForUpdate }, new XmlAsVarcharMaxSqlServerTestModell()));

            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([VarcharMAXColumn], [NvarcharMAXColumn]) VALUES(@varcharMAXColumn, @nvarcharMAXColumn)";
                    sqlCommand.Parameters.AddWithValue("@varcharMAXColumn", CheckValues[ChangeType.Insert.ToString()].Item1.VarcharMaxColumn);
                    sqlCommand.Parameters.AddWithValue("@nvarcharMAXColumn", CheckValues[ChangeType.Insert.ToString()].Item1.NvarcharMaxColumn);
                    sqlCommand.ExecuteNonQuery();

                    Thread.Sleep(1000);

                    sqlCommand.Parameters.Clear();
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [VarcharMAXColumn] = @varcharMAXColumn, [NvarcharMAXColumn] = @nvarcharMAXColumn";
                    sqlCommand.Parameters.AddWithValue("@varcharMAXColumn", CheckValues[ChangeType.Update.ToString()].Item1.VarcharMaxColumn);
                    sqlCommand.Parameters.AddWithValue("@nvarcharMAXColumn", CheckValues[ChangeType.Update.ToString()].Item1.NvarcharMaxColumn);
                    sqlCommand.ExecuteNonQuery();

                    Thread.Sleep(1000);

                    sqlCommand.Parameters.Clear();
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();

                    Thread.Sleep(1000);
                }
            }
        }
    }
}