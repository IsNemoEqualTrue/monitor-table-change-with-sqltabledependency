using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient;

namespace TableDependency.IntegrationTest.TypeChecks
{
    public class BinaryBitCharVarbinaryModel
    {
        public byte[] binary50Column { get; set; }
        public bool? bitColumn { get; set; }
        public bool bit2Column { get; set; }
        public bool bit3Column { get; set; }
        public char[] char10Column { get; set; }
        public byte[] varbinary50Column { get; set; }
        public byte[] varbinaryMAXColumn { get; set; }
    }

    [TestClass]
    public class BinaryBitCharVarbinaryTypes
    {
        private static string _connectionString = ConfigurationManager.ConnectionStrings["SqlServerConnectionString"].ConnectionString;
        private static string TableName = "Test";
        private static Dictionary<string, Tuple<BinaryBitCharVarbinaryModel, BinaryBitCharVarbinaryModel>> _checkValues = new Dictionary<string, Tuple<BinaryBitCharVarbinaryModel, BinaryBitCharVarbinaryModel>>();

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
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
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
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
            SqlTableDependency<BinaryBitCharVarbinaryModel> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<BinaryBitCharVarbinaryModel>(_connectionString, TableName);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(GetString(_checkValues[ChangeType.Insert.ToString()].Item2.binary50Column), GetString(_checkValues[ChangeType.Insert.ToString()].Item1.binary50Column));
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.bitColumn, _checkValues[ChangeType.Insert.ToString()].Item1.bitColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.char10Column, _checkValues[ChangeType.Insert.ToString()].Item1.char10Column);
            Assert.AreEqual(GetString(_checkValues[ChangeType.Insert.ToString()].Item2.varbinary50Column), GetString(_checkValues[ChangeType.Insert.ToString()].Item1.varbinary50Column));
            Assert.AreEqual(GetString(_checkValues[ChangeType.Insert.ToString()].Item2.varbinaryMAXColumn), GetString(_checkValues[ChangeType.Insert.ToString()].Item1.varbinaryMAXColumn));

            Assert.AreEqual(GetString(_checkValues[ChangeType.Update.ToString()].Item2.binary50Column), GetString(_checkValues[ChangeType.Update.ToString()].Item1.binary50Column));
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.bitColumn, _checkValues[ChangeType.Update.ToString()].Item1.bitColumn);
            Assert.AreEqual(new String(_checkValues[ChangeType.Update.ToString()].Item2.char10Column).Trim(), new String(_checkValues[ChangeType.Update.ToString()].Item1.char10Column).Trim());
            Assert.AreEqual(GetString(_checkValues[ChangeType.Update.ToString()].Item2.varbinary50Column), GetString(_checkValues[ChangeType.Update.ToString()].Item1.varbinary50Column));
            Assert.AreEqual(GetString(_checkValues[ChangeType.Update.ToString()].Item2.varbinaryMAXColumn), GetString(_checkValues[ChangeType.Update.ToString()].Item1.varbinaryMAXColumn));

            Assert.AreEqual(GetString(_checkValues[ChangeType.Delete.ToString()].Item2.binary50Column), GetString(_checkValues[ChangeType.Delete.ToString()].Item1.binary50Column));
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.bitColumn, _checkValues[ChangeType.Delete.ToString()].Item1.bitColumn);
            Assert.AreEqual(new String(_checkValues[ChangeType.Delete.ToString()].Item2.char10Column).Trim(), new String(_checkValues[ChangeType.Delete.ToString()].Item1.char10Column).Trim());
            Assert.AreEqual(GetString(_checkValues[ChangeType.Delete.ToString()].Item2.varbinary50Column), GetString(_checkValues[ChangeType.Delete.ToString()].Item1.varbinary50Column));
            Assert.AreEqual(GetString(_checkValues[ChangeType.Delete.ToString()].Item2.varbinaryMAXColumn), GetString(_checkValues[ChangeType.Delete.ToString()].Item1.varbinaryMAXColumn));

        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<BinaryBitCharVarbinaryModel> e)
        {

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.bitColumn = e.Entity.bitColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.bit2Column = e.Entity.bit2Column;
                    _checkValues[ChangeType.Insert.ToString()].Item2.bit3Column = e.Entity.bit3Column;
                    _checkValues[ChangeType.Insert.ToString()].Item2.binary50Column = e.Entity.binary50Column;
                    _checkValues[ChangeType.Insert.ToString()].Item2.char10Column = e.Entity.char10Column;
                    _checkValues[ChangeType.Insert.ToString()].Item2.varbinary50Column = e.Entity.varbinary50Column;
                    _checkValues[ChangeType.Insert.ToString()].Item2.varbinaryMAXColumn = e.Entity.varbinaryMAXColumn;

                    break;
                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.bitColumn = e.Entity.bitColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.bit2Column = e.Entity.bit2Column;
                    _checkValues[ChangeType.Update.ToString()].Item2.bit3Column = e.Entity.bit3Column;
                    _checkValues[ChangeType.Update.ToString()].Item2.binary50Column = e.Entity.binary50Column;
                    _checkValues[ChangeType.Update.ToString()].Item2.char10Column = e.Entity.char10Column;
                    _checkValues[ChangeType.Update.ToString()].Item2.varbinary50Column = e.Entity.varbinary50Column;
                    _checkValues[ChangeType.Update.ToString()].Item2.varbinaryMAXColumn = e.Entity.varbinaryMAXColumn;
                    break;
                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.bitColumn = e.Entity.bitColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.bit2Column = e.Entity.bit2Column;
                    _checkValues[ChangeType.Delete.ToString()].Item2.bit3Column = e.Entity.bit3Column;
                    _checkValues[ChangeType.Delete.ToString()].Item2.binary50Column = e.Entity.binary50Column;
                    _checkValues[ChangeType.Delete.ToString()].Item2.char10Column = e.Entity.char10Column;
                    _checkValues[ChangeType.Delete.ToString()].Item2.varbinary50Column = e.Entity.varbinary50Column;
                    _checkValues[ChangeType.Delete.ToString()].Item2.varbinaryMAXColumn = e.Entity.varbinaryMAXColumn;
                    break;
            }
        }

        static byte[] GetBytes(string str, int? lenght = null) 
        {
            if (str == null) return null;

            byte[] bytes = lenght.HasValue ? new byte[lenght.Value] : new byte[str.Length * sizeof(char)];
            Buffer.BlockCopy(str.ToCharArray(), 0, bytes, 0, str.Length * sizeof(char));
            return bytes;
        }

        static string GetString(byte[] bytes)
        {
            if (bytes == null) return null;

            char[] chars = new char[bytes.Length / sizeof(char)];
            System.Buffer.BlockCopy(bytes, 0, chars, 0, bytes.Length);
            return new string(chars);
        }

        private static void ModifyTableContent()
        {
            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<BinaryBitCharVarbinaryModel, BinaryBitCharVarbinaryModel>(new BinaryBitCharVarbinaryModel { binary50Column = GetBytes("Aurelia", 50), bit2Column = false, bit3Column = false, bitColumn = false, char10Column = null, varbinary50Column = GetBytes("Nonna"), varbinaryMAXColumn = null }, new BinaryBitCharVarbinaryModel()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<BinaryBitCharVarbinaryModel, BinaryBitCharVarbinaryModel>(new BinaryBitCharVarbinaryModel { binary50Column = GetBytes("Valentina", 50), bit2Column = true, bit3Column = true, bitColumn = true,  char10Column = new char[] { 'A' }, varbinary50Column = null, varbinaryMAXColumn = GetBytes("Velia") }, new BinaryBitCharVarbinaryModel()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<BinaryBitCharVarbinaryModel, BinaryBitCharVarbinaryModel>(new BinaryBitCharVarbinaryModel { binary50Column = GetBytes("Valentina", 50), bit2Column = true, bit3Column = true,  bitColumn = true,  char10Column = new char[] { 'A' }, varbinary50Column = null, varbinaryMAXColumn = GetBytes("Velia") }, new BinaryBitCharVarbinaryModel()));

            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([binary50Column], [bitColumn], [bit2Column], [bit3Column], [char10Column], varbinary50Column, varbinaryMAXColumn) VALUES (@binary50Column, @bitColumn, 0, 0, null, @varbinary50Column, null)";
                    sqlCommand.Parameters.Add(new SqlParameter("@binary50Column", SqlDbType.Binary) { Size = 50, Value = _checkValues[ChangeType.Insert.ToString()].Item1.binary50Column });
                    sqlCommand.Parameters.Add(new SqlParameter("@bitColumn", SqlDbType.Bit) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.bitColumn.GetValueOrDefault() });
                    sqlCommand.Parameters.Add(new SqlParameter("@varbinary50Column", SqlDbType.VarBinary) { Size = 50, Value = _checkValues[ChangeType.Insert.ToString()].Item1.varbinary50Column });
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [binary50Column] = @binary50Column, [bitColumn] = @bitColumn, [bit2Column] = 1, [bit3Column] = 1 ,[char10Column] = @char10Column, varbinary50Column = null, varbinaryMAXColumn = @varbinaryMAXColumn";
                    sqlCommand.Parameters.Add(new SqlParameter("@binary50Column", SqlDbType.Binary) { Value = _checkValues[ChangeType.Update.ToString()].Item1.binary50Column });
                    sqlCommand.Parameters.Add(new SqlParameter("@bitColumn", SqlDbType.Bit) { Value = _checkValues[ChangeType.Update.ToString()].Item1.bitColumn.GetValueOrDefault() });
                    sqlCommand.Parameters.Add(new SqlParameter("@char10Column", SqlDbType.Char) { Size = 10, Value = _checkValues[ChangeType.Update.ToString()].Item1.char10Column });
                    sqlCommand.Parameters.Add(new SqlParameter("@varbinaryMAXColumn", SqlDbType.VarBinary) { Value = _checkValues[ChangeType.Update.ToString()].Item1.varbinaryMAXColumn });
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);
                }
            }
        }
    }
}