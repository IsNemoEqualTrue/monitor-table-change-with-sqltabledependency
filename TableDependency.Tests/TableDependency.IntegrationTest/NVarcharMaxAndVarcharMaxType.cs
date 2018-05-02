using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.IntegrationTest.Base;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest
{
    public class NVarcharMaxAndVarcharMaxTypeModel
    {
        // *****************************************************
        // SQL Server Data Type Mappings: 
        // https://msdn.microsoft.com/en-us/library/cc716729%28v=vs.110%29.aspx?f=255&MSPPError=-2147217396
        // *****************************************************
        public string VarcharMaxColumn { get; set; }
        public string NvarcharMaxColumn { get; set; }
    }

    [TestClass]
    public class NVarcharMaxAndVarcharMaxType : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(NVarcharMaxAndVarcharMaxTypeModel).Name;
        private static readonly Dictionary<string, Tuple<NVarcharMaxAndVarcharMaxTypeModel, NVarcharMaxAndVarcharMaxTypeModel>> CheckValues = new Dictionary<string, Tuple<NVarcharMaxAndVarcharMaxTypeModel, NVarcharMaxAndVarcharMaxTypeModel>>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
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

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
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
            SqlTableDependency<NVarcharMaxAndVarcharMaxTypeModel> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<NVarcharMaxAndVarcharMaxTypeModel>(ConnectionStringForTestUser, tableName: TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent1);
                t.Start();
                Thread.Sleep(1000 * 10 * 1);
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

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming)== 0);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<NVarcharMaxAndVarcharMaxTypeModel> e)
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
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<NVarcharMaxAndVarcharMaxTypeModel, NVarcharMaxAndVarcharMaxTypeModel>(new NVarcharMaxAndVarcharMaxTypeModel { VarcharMaxColumn = new string('*', 6000), NvarcharMaxColumn = new string('*', 8000) }, new NVarcharMaxAndVarcharMaxTypeModel()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<NVarcharMaxAndVarcharMaxTypeModel, NVarcharMaxAndVarcharMaxTypeModel>(new NVarcharMaxAndVarcharMaxTypeModel { VarcharMaxColumn = "111", NvarcharMaxColumn = "new byte[] { 1, 2, 3, 4, 5, 6 }" }, new NVarcharMaxAndVarcharMaxTypeModel()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<NVarcharMaxAndVarcharMaxTypeModel, NVarcharMaxAndVarcharMaxTypeModel>(new NVarcharMaxAndVarcharMaxTypeModel { VarcharMaxColumn = "111", NvarcharMaxColumn = "new byte[] { 1, 2, 3, 4, 5, 6 }" }, new NVarcharMaxAndVarcharMaxTypeModel()));

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([varcharMAXColumn], [nvarcharMAXColumn]) VALUES(@varcharMAXColumn, @nvarcharMAXColumn)";
                    sqlCommand.Parameters.AddWithValue("@varcharMAXColumn", CheckValues[ChangeType.Insert.ToString()].Item1.VarcharMaxColumn);
                    sqlCommand.Parameters.AddWithValue("@nvarcharMAXColumn", CheckValues[ChangeType.Insert.ToString()].Item1.NvarcharMaxColumn);
                    sqlCommand.ExecuteNonQuery();
                }

                Thread.Sleep(1000);

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [varcharMAXColumn] = @varcharMAXColumn, [nvarcharMAXColumn] = @nvarcharMAXColumn";
                    sqlCommand.Parameters.AddWithValue("@varcharMAXColumn", CheckValues[ChangeType.Update.ToString()].Item1.VarcharMaxColumn);
                    sqlCommand.Parameters.AddWithValue("@nvarcharMAXColumn", CheckValues[ChangeType.Update.ToString()].Item1.NvarcharMaxColumn);
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