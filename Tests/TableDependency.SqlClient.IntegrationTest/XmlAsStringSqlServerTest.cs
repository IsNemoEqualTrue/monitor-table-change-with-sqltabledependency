using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient.BaseTests;

namespace TableDependency.SqlClient.IntegrationTests
{
    public class XmlAsStringSqlServerTestModel
    {
        // *****************************************************
        // SQL Server Data Type Mappings: 
        // https://msdn.microsoft.com/en-us/library/cc716729%28v=vs.110%29.aspx?f=255&MSPPError=-2147217396
        // *****************************************************
        public string VarcharMaxColumn { get; set; }
        public string NvarcharMaxColumn { get; set; }
    }

    [TestClass]
    public class XmlAsStringSqlServerTest : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(XmlAsStringSqlServerTestModel).Name;
        private static Dictionary<string, Tuple<XmlAsStringSqlServerTestModel, XmlAsStringSqlServerTestModel>> _checkValues = new Dictionary<string, Tuple<XmlAsStringSqlServerTestModel, XmlAsStringSqlServerTestModel>>();
        private static Dictionary<string, Tuple<XmlAsStringSqlServerTestModel, XmlAsStringSqlServerTestModel>> _checkValuesOld = new Dictionary<string, Tuple<XmlAsStringSqlServerTestModel, XmlAsStringSqlServerTestModel>>();

        private const string XmlForInsert = @"<?xml version=""1.0"" encoding=""utf-8""?><catalog><book id=""1""><author>Gambardella, Matthew</author><title>XML Developer's Guide</title></book></catalog>";
        private const string XmlForUpdate = @"<?xml version=""1.0"" encoding=""utf-8""?><catalog><book id=""2""><author>Ridley, Matthew</author><title>XML Developer's Guide</title></book></catalog>";

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
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

        [TestInitialize]
        public void TestInitialize()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }

            _checkValues.Clear();
            _checkValuesOld.Clear();

            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<XmlAsStringSqlServerTestModel, XmlAsStringSqlServerTestModel>(new XmlAsStringSqlServerTestModel { VarcharMaxColumn = XmlForInsert, NvarcharMaxColumn = XmlForInsert }, new XmlAsStringSqlServerTestModel()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<XmlAsStringSqlServerTestModel, XmlAsStringSqlServerTestModel>(new XmlAsStringSqlServerTestModel { VarcharMaxColumn = XmlForUpdate, NvarcharMaxColumn = XmlForUpdate }, new XmlAsStringSqlServerTestModel()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<XmlAsStringSqlServerTestModel, XmlAsStringSqlServerTestModel>(new XmlAsStringSqlServerTestModel { VarcharMaxColumn = XmlForUpdate, NvarcharMaxColumn = XmlForUpdate }, new XmlAsStringSqlServerTestModel()));

            _checkValuesOld.Add(ChangeType.Insert.ToString(), new Tuple<XmlAsStringSqlServerTestModel, XmlAsStringSqlServerTestModel>(new XmlAsStringSqlServerTestModel { VarcharMaxColumn = XmlForInsert, NvarcharMaxColumn = XmlForInsert }, new XmlAsStringSqlServerTestModel()));
            _checkValuesOld.Add(ChangeType.Update.ToString(), new Tuple<XmlAsStringSqlServerTestModel, XmlAsStringSqlServerTestModel>(new XmlAsStringSqlServerTestModel { VarcharMaxColumn = XmlForUpdate, NvarcharMaxColumn = XmlForUpdate }, new XmlAsStringSqlServerTestModel()));
            _checkValuesOld.Add(ChangeType.Delete.ToString(), new Tuple<XmlAsStringSqlServerTestModel, XmlAsStringSqlServerTestModel>(new XmlAsStringSqlServerTestModel { VarcharMaxColumn = XmlForUpdate, NvarcharMaxColumn = XmlForUpdate }, new XmlAsStringSqlServerTestModel()));
        }

        [ClassCleanup]
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
        public void Test()
        {
            SqlTableDependency<XmlAsStringSqlServerTestModel> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<XmlAsStringSqlServerTestModel>(ConnectionStringForTestUser);
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

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.VarcharMaxColumn, _checkValues[ChangeType.Insert.ToString()].Item1.VarcharMaxColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.NvarcharMaxColumn, _checkValues[ChangeType.Insert.ToString()].Item1.NvarcharMaxColumn);
            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.VarcharMaxColumn, _checkValues[ChangeType.Update.ToString()].Item1.VarcharMaxColumn);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.NvarcharMaxColumn, _checkValues[ChangeType.Update.ToString()].Item1.NvarcharMaxColumn);
            Assert.IsNull(_checkValuesOld[ChangeType.Update.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.VarcharMaxColumn, _checkValues[ChangeType.Delete.ToString()].Item1.VarcharMaxColumn);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.NvarcharMaxColumn, _checkValues[ChangeType.Delete.ToString()].Item1.NvarcharMaxColumn);
            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);

            Thread.Sleep(1000 * 10 * 1);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TestWithOldValues()
        {
            SqlTableDependency<XmlAsStringSqlServerTestModel> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<XmlAsStringSqlServerTestModel>(ConnectionStringForTestUser, includeOldValues: true);
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

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.VarcharMaxColumn, _checkValues[ChangeType.Insert.ToString()].Item1.VarcharMaxColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.NvarcharMaxColumn, _checkValues[ChangeType.Insert.ToString()].Item1.NvarcharMaxColumn);
            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.VarcharMaxColumn, _checkValues[ChangeType.Update.ToString()].Item1.VarcharMaxColumn);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.NvarcharMaxColumn, _checkValues[ChangeType.Update.ToString()].Item1.NvarcharMaxColumn);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.VarcharMaxColumn, _checkValues[ChangeType.Insert.ToString()].Item2.VarcharMaxColumn);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.NvarcharMaxColumn, _checkValues[ChangeType.Insert.ToString()].Item2.NvarcharMaxColumn);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.VarcharMaxColumn, _checkValues[ChangeType.Delete.ToString()].Item1.VarcharMaxColumn);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.NvarcharMaxColumn, _checkValues[ChangeType.Delete.ToString()].Item1.NvarcharMaxColumn);
            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);

            Thread.Sleep(1000 * 10 * 1);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<XmlAsStringSqlServerTestModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.VarcharMaxColumn = e.Entity.VarcharMaxColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.NvarcharMaxColumn = e.Entity.NvarcharMaxColumn;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.VarcharMaxColumn = e.EntityOldValues.VarcharMaxColumn;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.NvarcharMaxColumn = e.EntityOldValues.NvarcharMaxColumn;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()] = null;
                    }

                    break;

                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.VarcharMaxColumn = e.Entity.VarcharMaxColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.NvarcharMaxColumn = e.Entity.NvarcharMaxColumn;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.VarcharMaxColumn = e.EntityOldValues.VarcharMaxColumn;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.NvarcharMaxColumn = e.EntityOldValues.NvarcharMaxColumn;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Update.ToString()] = null;
                    }

                    break;

                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.VarcharMaxColumn = e.Entity.VarcharMaxColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.NvarcharMaxColumn = e.Entity.NvarcharMaxColumn;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.VarcharMaxColumn = e.EntityOldValues.VarcharMaxColumn;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.NvarcharMaxColumn = e.EntityOldValues.NvarcharMaxColumn;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Delete.ToString()] = null;
                    }

                    break;
            }
        }

        private static void ModifyTableContent1()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([VarcharMAXColumn], [NvarcharMAXColumn]) VALUES(@varcharMAXColumn, @nvarcharMAXColumn)";
                    sqlCommand.Parameters.AddWithValue("@varcharMAXColumn", _checkValues[ChangeType.Insert.ToString()].Item1.VarcharMaxColumn);
                    sqlCommand.Parameters.AddWithValue("@nvarcharMAXColumn", _checkValues[ChangeType.Insert.ToString()].Item1.NvarcharMaxColumn);
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.Parameters.Clear();
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [VarcharMAXColumn] = @varcharMAXColumn, [NvarcharMAXColumn] = @nvarcharMAXColumn";
                    sqlCommand.Parameters.AddWithValue("@varcharMAXColumn", _checkValues[ChangeType.Update.ToString()].Item1.VarcharMaxColumn);
                    sqlCommand.Parameters.AddWithValue("@nvarcharMAXColumn", _checkValues[ChangeType.Update.ToString()].Item1.NvarcharMaxColumn);
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.Parameters.Clear();
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}