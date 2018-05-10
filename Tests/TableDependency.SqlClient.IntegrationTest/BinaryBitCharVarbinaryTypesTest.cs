using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient.BaseTests;

namespace TableDependency.SqlClient.IntegrationTests
{
    public class BinaryBitCharVarbinaryTypesModel
    {
        public byte[] Binary50Column { get; set; }
        public bool? BitColumn { get; set; }
        public bool Bit2Column { get; set; }
        public bool Bit3Column { get; set; }
        public char[] Char10Column { get; set; }
        public byte[] Varbinary50Column { get; set; }
        public byte[] VarbinaryMaxColumn { get; set; }
    }

    [TestClass]
    public class BinaryBitCharVarbinaryTypesTest : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = "Test";
        private static Dictionary<string, Tuple<BinaryBitCharVarbinaryTypesModel, BinaryBitCharVarbinaryTypesModel>> _checkValues = new Dictionary<string, Tuple<BinaryBitCharVarbinaryTypesModel, BinaryBitCharVarbinaryTypesModel>>();
        private static Dictionary<string, Tuple<BinaryBitCharVarbinaryTypesModel, BinaryBitCharVarbinaryTypesModel>> _checkValuesOld = new Dictionary<string, Tuple<BinaryBitCharVarbinaryTypesModel, BinaryBitCharVarbinaryTypesModel>>();

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

                    sqlCommand.CommandText = $"CREATE TABLE {TableName} (" +
                        "binary50Column binary(50) NULL, " +
                        "bitColumn bit NULL, bit2Column BIT, bit3Column BIT," +
                        "char10Column char(10) NULL, " +
                        "varbinary50Column varbinary(50) NULL, " +
                        "varbinaryMAXColumn varbinary(MAX) NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize()]
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

            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<BinaryBitCharVarbinaryTypesModel, BinaryBitCharVarbinaryTypesModel>(new BinaryBitCharVarbinaryTypesModel { Binary50Column = GetBytes("Aurelia", 50), Bit2Column = false, Bit3Column = false, BitColumn = false, Char10Column = null, Varbinary50Column = GetBytes("Nonna"), VarbinaryMaxColumn = null }, new BinaryBitCharVarbinaryTypesModel()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<BinaryBitCharVarbinaryTypesModel, BinaryBitCharVarbinaryTypesModel>(new BinaryBitCharVarbinaryTypesModel { Binary50Column = GetBytes("Valentina", 50), Bit2Column = true, Bit3Column = true, BitColumn = true, Char10Column = new char[] { 'A' }, Varbinary50Column = null, VarbinaryMaxColumn = GetBytes("Velia") }, new BinaryBitCharVarbinaryTypesModel()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<BinaryBitCharVarbinaryTypesModel, BinaryBitCharVarbinaryTypesModel>(new BinaryBitCharVarbinaryTypesModel { Binary50Column = GetBytes("Valentina", 50), Bit2Column = true, Bit3Column = true, BitColumn = true, Char10Column = new char[] { 'A' }, Varbinary50Column = null, VarbinaryMaxColumn = GetBytes("Velia") }, new BinaryBitCharVarbinaryTypesModel()));

            _checkValuesOld.Add(ChangeType.Insert.ToString(), new Tuple<BinaryBitCharVarbinaryTypesModel, BinaryBitCharVarbinaryTypesModel>(new BinaryBitCharVarbinaryTypesModel { Binary50Column = GetBytes("Aurelia", 50), Bit2Column = false, Bit3Column = false, BitColumn = false, Char10Column = null, Varbinary50Column = GetBytes("Nonna"), VarbinaryMaxColumn = null }, new BinaryBitCharVarbinaryTypesModel()));
            _checkValuesOld.Add(ChangeType.Update.ToString(), new Tuple<BinaryBitCharVarbinaryTypesModel, BinaryBitCharVarbinaryTypesModel>(new BinaryBitCharVarbinaryTypesModel { Binary50Column = GetBytes("Valentina", 50), Bit2Column = true, Bit3Column = true, BitColumn = true, Char10Column = new char[] { 'A' }, Varbinary50Column = null, VarbinaryMaxColumn = GetBytes("Velia") }, new BinaryBitCharVarbinaryTypesModel()));
            _checkValuesOld.Add(ChangeType.Delete.ToString(), new Tuple<BinaryBitCharVarbinaryTypesModel, BinaryBitCharVarbinaryTypesModel>(new BinaryBitCharVarbinaryTypesModel { Binary50Column = GetBytes("Valentina", 50), Bit2Column = true, Bit3Column = true, BitColumn = true, Char10Column = new char[] { 'A' }, Varbinary50Column = null, VarbinaryMaxColumn = GetBytes("Velia") }, new BinaryBitCharVarbinaryTypesModel()));
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
        public void Test()
        {
            SqlTableDependency<BinaryBitCharVarbinaryTypesModel> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<BinaryBitCharVarbinaryTypesModel>(ConnectionStringForTestUser, tableName: TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 5 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(GetString(_checkValues[ChangeType.Insert.ToString()].Item2.Binary50Column), GetString(_checkValues[ChangeType.Insert.ToString()].Item1.Binary50Column));
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.BitColumn, _checkValues[ChangeType.Insert.ToString()].Item1.BitColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Char10Column, _checkValues[ChangeType.Insert.ToString()].Item1.Char10Column);
            Assert.AreEqual(GetString(_checkValues[ChangeType.Insert.ToString()].Item2.Varbinary50Column), GetString(_checkValues[ChangeType.Insert.ToString()].Item1.Varbinary50Column));
            Assert.AreEqual(GetString(_checkValues[ChangeType.Insert.ToString()].Item2.VarbinaryMaxColumn), GetString(_checkValues[ChangeType.Insert.ToString()].Item1.VarbinaryMaxColumn));

            Assert.AreEqual(GetString(_checkValues[ChangeType.Update.ToString()].Item2.Binary50Column), GetString(_checkValues[ChangeType.Update.ToString()].Item1.Binary50Column));
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.BitColumn, _checkValues[ChangeType.Update.ToString()].Item1.BitColumn);
            Assert.AreEqual(new String(_checkValues[ChangeType.Update.ToString()].Item2.Char10Column).Trim(), new String(_checkValues[ChangeType.Update.ToString()].Item1.Char10Column).Trim());
            Assert.AreEqual(GetString(_checkValues[ChangeType.Update.ToString()].Item2.Varbinary50Column), GetString(_checkValues[ChangeType.Update.ToString()].Item1.Varbinary50Column));
            Assert.AreEqual(GetString(_checkValues[ChangeType.Update.ToString()].Item2.VarbinaryMaxColumn), GetString(_checkValues[ChangeType.Update.ToString()].Item1.VarbinaryMaxColumn));

            Assert.AreEqual(GetString(_checkValues[ChangeType.Delete.ToString()].Item2.Binary50Column), GetString(_checkValues[ChangeType.Delete.ToString()].Item1.Binary50Column));
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.BitColumn, _checkValues[ChangeType.Delete.ToString()].Item1.BitColumn);
            Assert.AreEqual(new String(_checkValues[ChangeType.Delete.ToString()].Item2.Char10Column).Trim(), new String(_checkValues[ChangeType.Delete.ToString()].Item1.Char10Column).Trim());
            Assert.AreEqual(GetString(_checkValues[ChangeType.Delete.ToString()].Item2.Varbinary50Column), GetString(_checkValues[ChangeType.Delete.ToString()].Item1.Varbinary50Column));
            Assert.AreEqual(GetString(_checkValues[ChangeType.Delete.ToString()].Item2.VarbinaryMaxColumn), GetString(_checkValues[ChangeType.Delete.ToString()].Item1.VarbinaryMaxColumn));

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TestWithOldValues()
        {
            SqlTableDependency<BinaryBitCharVarbinaryTypesModel> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<BinaryBitCharVarbinaryTypesModel>(
                    ConnectionStringForTestUser,
                    TableName,
                    includeOldValues: true);

                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 10 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(GetString(_checkValues[ChangeType.Insert.ToString()].Item2.Binary50Column), GetString(_checkValues[ChangeType.Insert.ToString()].Item1.Binary50Column));
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.BitColumn, _checkValues[ChangeType.Insert.ToString()].Item1.BitColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Char10Column, _checkValues[ChangeType.Insert.ToString()].Item1.Char10Column);
            Assert.AreEqual(GetString(_checkValues[ChangeType.Insert.ToString()].Item2.Varbinary50Column), GetString(_checkValues[ChangeType.Insert.ToString()].Item1.Varbinary50Column));
            Assert.AreEqual(GetString(_checkValues[ChangeType.Insert.ToString()].Item2.VarbinaryMaxColumn), GetString(_checkValues[ChangeType.Insert.ToString()].Item1.VarbinaryMaxColumn));

            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(GetString(_checkValues[ChangeType.Update.ToString()].Item2.Binary50Column), GetString(_checkValues[ChangeType.Update.ToString()].Item1.Binary50Column));
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.BitColumn, _checkValues[ChangeType.Update.ToString()].Item1.BitColumn);
            Assert.AreEqual(new String(_checkValues[ChangeType.Update.ToString()].Item2.Char10Column).Trim(), new String(_checkValues[ChangeType.Update.ToString()].Item1.Char10Column).Trim());
            Assert.AreEqual(GetString(_checkValues[ChangeType.Update.ToString()].Item2.Varbinary50Column), GetString(_checkValues[ChangeType.Update.ToString()].Item1.Varbinary50Column));
            Assert.AreEqual(GetString(_checkValues[ChangeType.Update.ToString()].Item2.VarbinaryMaxColumn), GetString(_checkValues[ChangeType.Update.ToString()].Item1.VarbinaryMaxColumn));

            Assert.AreEqual(GetString(_checkValuesOld[ChangeType.Update.ToString()].Item2.Binary50Column), GetString(_checkValues[ChangeType.Insert.ToString()].Item2.Binary50Column));
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.BitColumn, _checkValues[ChangeType.Insert.ToString()].Item2.BitColumn);
            Assert.AreEqual(new String(_checkValuesOld[ChangeType.Update.ToString()].Item2.Char10Column).Trim(), new String(_checkValues[ChangeType.Insert.ToString()].Item2.Char10Column).Trim());
            Assert.AreEqual(GetString(_checkValuesOld[ChangeType.Update.ToString()].Item2.Varbinary50Column), GetString(_checkValues[ChangeType.Insert.ToString()].Item2.Varbinary50Column));
            Assert.AreEqual(GetString(_checkValuesOld[ChangeType.Update.ToString()].Item2.VarbinaryMaxColumn), GetString(_checkValues[ChangeType.Insert.ToString()].Item2.VarbinaryMaxColumn));

            Assert.AreEqual(GetString(_checkValues[ChangeType.Delete.ToString()].Item2.Binary50Column), GetString(_checkValues[ChangeType.Delete.ToString()].Item1.Binary50Column));
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.BitColumn, _checkValues[ChangeType.Delete.ToString()].Item1.BitColumn);
            Assert.AreEqual(new String(_checkValues[ChangeType.Delete.ToString()].Item2.Char10Column).Trim(), new String(_checkValues[ChangeType.Delete.ToString()].Item1.Char10Column).Trim());
            Assert.AreEqual(GetString(_checkValues[ChangeType.Delete.ToString()].Item2.Varbinary50Column), GetString(_checkValues[ChangeType.Delete.ToString()].Item1.Varbinary50Column));
            Assert.AreEqual(GetString(_checkValues[ChangeType.Delete.ToString()].Item2.VarbinaryMaxColumn), GetString(_checkValues[ChangeType.Delete.ToString()].Item1.VarbinaryMaxColumn));

            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<BinaryBitCharVarbinaryTypesModel> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.BitColumn = e.Entity.BitColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.Bit2Column = e.Entity.Bit2Column;
                    _checkValues[ChangeType.Insert.ToString()].Item2.Bit3Column = e.Entity.Bit3Column;
                    _checkValues[ChangeType.Insert.ToString()].Item2.Binary50Column = e.Entity.Binary50Column;
                    _checkValues[ChangeType.Insert.ToString()].Item2.Char10Column = e.Entity.Char10Column;
                    _checkValues[ChangeType.Insert.ToString()].Item2.Varbinary50Column = e.Entity.Varbinary50Column;
                    _checkValues[ChangeType.Insert.ToString()].Item2.VarbinaryMaxColumn = e.Entity.VarbinaryMaxColumn;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.BitColumn = e.EntityOldValues.BitColumn;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.Bit2Column = e.EntityOldValues.Bit2Column;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.Bit3Column = e.EntityOldValues.Bit3Column;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.Binary50Column = e.EntityOldValues.Binary50Column;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.Char10Column = e.EntityOldValues.Char10Column;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.Varbinary50Column = e.EntityOldValues.Varbinary50Column;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.VarbinaryMaxColumn = e.EntityOldValues.VarbinaryMaxColumn;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()] = null;
                    }

                    break;

                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.BitColumn = e.Entity.BitColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.Bit2Column = e.Entity.Bit2Column;
                    _checkValues[ChangeType.Update.ToString()].Item2.Bit3Column = e.Entity.Bit3Column;
                    _checkValues[ChangeType.Update.ToString()].Item2.Binary50Column = e.Entity.Binary50Column;
                    _checkValues[ChangeType.Update.ToString()].Item2.Char10Column = e.Entity.Char10Column;
                    _checkValues[ChangeType.Update.ToString()].Item2.Varbinary50Column = e.Entity.Varbinary50Column;
                    _checkValues[ChangeType.Update.ToString()].Item2.VarbinaryMaxColumn = e.Entity.VarbinaryMaxColumn;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.BitColumn = e.EntityOldValues.BitColumn;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.Bit2Column = e.EntityOldValues.Bit2Column;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.Bit3Column = e.EntityOldValues.Bit3Column;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.Binary50Column = e.EntityOldValues.Binary50Column;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.Char10Column = e.EntityOldValues.Char10Column;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.Varbinary50Column = e.EntityOldValues.Varbinary50Column;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.VarbinaryMaxColumn = e.EntityOldValues.VarbinaryMaxColumn;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Update.ToString()] = null;
                    }

                    break;

                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.BitColumn = e.Entity.BitColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.Bit2Column = e.Entity.Bit2Column;
                    _checkValues[ChangeType.Delete.ToString()].Item2.Bit3Column = e.Entity.Bit3Column;
                    _checkValues[ChangeType.Delete.ToString()].Item2.Binary50Column = e.Entity.Binary50Column;
                    _checkValues[ChangeType.Delete.ToString()].Item2.Char10Column = e.Entity.Char10Column;
                    _checkValues[ChangeType.Delete.ToString()].Item2.Varbinary50Column = e.Entity.Varbinary50Column;
                    _checkValues[ChangeType.Delete.ToString()].Item2.VarbinaryMaxColumn = e.Entity.VarbinaryMaxColumn;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.BitColumn = e.EntityOldValues.BitColumn;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.Bit2Column = e.EntityOldValues.Bit2Column;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.Bit3Column = e.EntityOldValues.Bit3Column;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.Binary50Column = e.EntityOldValues.Binary50Column;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.Char10Column = e.EntityOldValues.Char10Column;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.Varbinary50Column = e.EntityOldValues.Varbinary50Column;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.VarbinaryMaxColumn = e.EntityOldValues.VarbinaryMaxColumn;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Delete.ToString()] = null;
                    }

                    break;
            }
        }

        private void ModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([binary50Column], [bitColumn], [bit2Column], [bit3Column], [char10Column], varbinary50Column, varbinaryMAXColumn) VALUES (@binary50Column, @bitColumn, 0, 0, null, @varbinary50Column, null)";
                    sqlCommand.Parameters.Add(new SqlParameter("@binary50Column", SqlDbType.Binary) { Size = 50, Value = _checkValues[ChangeType.Insert.ToString()].Item1.Binary50Column });
                    sqlCommand.Parameters.Add(new SqlParameter("@bitColumn", SqlDbType.Bit) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.BitColumn.GetValueOrDefault() });
                    sqlCommand.Parameters.Add(new SqlParameter("@varbinary50Column", SqlDbType.VarBinary) { Size = 50, Value = _checkValues[ChangeType.Insert.ToString()].Item1.Varbinary50Column });
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [binary50Column] = @binary50Column, [bitColumn] = @bitColumn, [bit2Column] = 1, [bit3Column] = 1 ,[char10Column] = @char10Column, varbinary50Column = null, varbinaryMAXColumn = @varbinaryMAXColumn";
                    sqlCommand.Parameters.Add(new SqlParameter("@binary50Column", SqlDbType.Binary) { Value = _checkValues[ChangeType.Update.ToString()].Item1.Binary50Column });
                    sqlCommand.Parameters.Add(new SqlParameter("@bitColumn", SqlDbType.Bit) { Value = _checkValues[ChangeType.Update.ToString()].Item1.BitColumn.GetValueOrDefault() });
                    sqlCommand.Parameters.Add(new SqlParameter("@char10Column", SqlDbType.Char) { Size = 10, Value = _checkValues[ChangeType.Update.ToString()].Item1.Char10Column });
                    sqlCommand.Parameters.Add(new SqlParameter("@varbinaryMAXColumn", SqlDbType.VarBinary) { Value = _checkValues[ChangeType.Update.ToString()].Item1.VarbinaryMaxColumn });
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}