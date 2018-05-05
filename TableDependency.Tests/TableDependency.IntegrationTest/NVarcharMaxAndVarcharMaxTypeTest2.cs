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
    public class NVarcharMaxAndVarcharMaxType2Model
    {
        // *****************************************************
        // SQL Server Data Type Mappings: 
        // https://msdn.microsoft.com/en-us/library/cc716729%28v=vs.110%29.aspx?f=255&MSPPError=-2147217396
        // *****************************************************
        public string VarcharMaxColumn { get; set; }
        public string NvarcharMaxColumn { get; set; }
    }

    [TestClass]
    public class NVarcharMaxAndVarcharMaxTypeTest2 : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(NVarcharMaxAndVarcharMaxType2Model).Name;
        private static readonly Dictionary<string, Tuple<NVarcharMaxAndVarcharMaxType2Model, NVarcharMaxAndVarcharMaxType2Model>> CheckValues = new Dictionary<string, Tuple<NVarcharMaxAndVarcharMaxType2Model, NVarcharMaxAndVarcharMaxType2Model>>();

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
            SqlTableDependency<NVarcharMaxAndVarcharMaxType2Model> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<NVarcharMaxAndVarcharMaxType2Model>(ConnectionStringForTestUser);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent1);
                t.Start();
                Thread.Sleep(1000 * 5 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.VarcharMaxColumn, CheckValues[ChangeType.Insert.ToString()].Item1.VarcharMaxColumn);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.NvarcharMaxColumn, CheckValues[ChangeType.Insert.ToString()].Item1.NvarcharMaxColumn);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming)== 0);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<NVarcharMaxAndVarcharMaxType2Model> e)
        {
            CheckValues[ChangeType.Insert.ToString()].Item2.VarcharMaxColumn = e.Entity.VarcharMaxColumn;
            CheckValues[ChangeType.Insert.ToString()].Item2.NvarcharMaxColumn = e.Entity.NvarcharMaxColumn;         
        }

        private static void ModifyTableContent1()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), 
                new Tuple<NVarcharMaxAndVarcharMaxType2Model, NVarcharMaxAndVarcharMaxType2Model>(new NVarcharMaxAndVarcharMaxType2Model
                { VarcharMaxColumn = new string('¢', 6000), NvarcharMaxColumn = "мы фантастические" }, new NVarcharMaxAndVarcharMaxType2Model()));
            
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
            }
        }
    }
}