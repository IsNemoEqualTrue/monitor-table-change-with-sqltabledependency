﻿using System;
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

namespace TableDependency.IntegrationTest
{
    [TestClass]
    public class NVarcharMaxAndVarcharMaxType2
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServer2008 Test_User"].ConnectionString;
        private static string TableName = "AXXTest";
        private static readonly Dictionary<string, Tuple<NVarcharMaxAndVarcharMaxModel2, NVarcharMaxAndVarcharMaxModel2>> CheckValues = new Dictionary<string, Tuple<NVarcharMaxAndVarcharMaxModel2, NVarcharMaxAndVarcharMaxModel2>>();

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
            SqlTableDependency<NVarcharMaxAndVarcharMaxModel2> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<NVarcharMaxAndVarcharMaxModel2>(ConnectionString, TableName);
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
                   
            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(SqlServerHelper.AreAllEndpointDisposed(naming));
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<NVarcharMaxAndVarcharMaxModel2> e)
        {
            CheckValues[ChangeType.Insert.ToString()].Item2.varcharMAXColumn = e.Entity.varcharMAXColumn;
            CheckValues[ChangeType.Insert.ToString()].Item2.nvarcharMAXColumn = e.Entity.nvarcharMAXColumn;         
        }

        private static void ModifyTableContent1()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), 
                new Tuple<NVarcharMaxAndVarcharMaxModel2, NVarcharMaxAndVarcharMaxModel2>(new NVarcharMaxAndVarcharMaxModel2
                { varcharMAXColumn = new string('¢', 6000), nvarcharMAXColumn = "мы фантастические" }, new NVarcharMaxAndVarcharMaxModel2()));
            
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
            }
        }
    }
}