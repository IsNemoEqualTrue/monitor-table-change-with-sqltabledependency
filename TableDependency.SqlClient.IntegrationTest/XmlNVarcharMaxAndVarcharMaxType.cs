using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient.IntegrationTest.Helpers;
using TableDependency.SqlClient.IntegrationTest.Model;

namespace TableDependency.SqlClient.IntegrationTest
{
    [TestClass]
    public class XmlNVarcharMaxAndVarcharMaxType
    {
        private static string _connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private static string TableName = "Test";
        private static Dictionary<string, Tuple<Check_Model, Check_Model>> _checkValues = new Dictionary<string, Tuple<Check_Model, Check_Model>>();

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

                    sqlCommand.CommandText = $"CREATE TABLE {TableName}(" +
                        "varcharMAXColumn VARCHAR(MAX) NULL, " +
                        "NvarcharMAXColumn NVARCHAR(MAX) NULL, " +
                        "xmlColumn XML NULL)";
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

        [TestMethod]
        public void ColumnTypesTest()
        {
            SqlTableDependency<Check_Model> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<Check_Model>(_connectionString, TableName);
                tableDependency.OnChanged += TableDependency_Changed;
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

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.varcharMAXColumn, _checkValues[ChangeType.Insert.ToString()].Item1.varcharMAXColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.nvarcharMAXColumn, _checkValues[ChangeType.Insert.ToString()].Item1.nvarcharMAXColumn);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.xmlColumn, _checkValues[ChangeType.Insert.ToString()].Item1.xmlColumn);

            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.varcharMAXColumn, _checkValues[ChangeType.Update.ToString()].Item1.varcharMAXColumn);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.nvarcharMAXColumn, _checkValues[ChangeType.Update.ToString()].Item1.nvarcharMAXColumn);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.xmlColumn, _checkValues[ChangeType.Update.ToString()].Item1.xmlColumn);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.varcharMAXColumn, _checkValues[ChangeType.Delete.ToString()].Item1.varcharMAXColumn);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.nvarcharMAXColumn, _checkValues[ChangeType.Delete.ToString()].Item1.nvarcharMAXColumn);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.xmlColumn, _checkValues[ChangeType.Update.ToString()].Item1.xmlColumn);

            Assert.IsTrue(Helper.AreAllDbObjectDisposed(_connectionString, naming));
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<Check_Model> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.varcharMAXColumn = e.Entity.varcharMAXColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.nvarcharMAXColumn = e.Entity.nvarcharMAXColumn;
                    _checkValues[ChangeType.Insert.ToString()].Item2.xmlColumn = e.Entity.xmlColumn;
                    break;
                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.varcharMAXColumn = e.Entity.varcharMAXColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.nvarcharMAXColumn = e.Entity.nvarcharMAXColumn;
                    _checkValues[ChangeType.Update.ToString()].Item2.xmlColumn = e.Entity.xmlColumn;
                    break;
                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.varcharMAXColumn = e.Entity.varcharMAXColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.nvarcharMAXColumn = e.Entity.nvarcharMAXColumn;
                    _checkValues[ChangeType.Delete.ToString()].Item2.xmlColumn = e.Entity.xmlColumn;
                    break;
            }
        }

        public static SqlXml ConvertString2SqlXml(string xmlData)
        {
            var encoding = new UTF8Encoding();
            var m = new MemoryStream(encoding.GetBytes(xmlData));
            return new SqlXml(m);
        }

        private static void ModifyTableContent()
        {
            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<Check_Model, Check_Model>(new Check_Model { varcharMAXColumn = "Lorem ipsum dolor sit amet, consectetur adipisci elit, sed eiusmod tempor incidunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrum exercitationem ullam corporis suscipit laboriosam, nisi ut aliquid ex ea commodi consequatur. Quis aute iure reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint obcaecat cupiditat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.", nvarcharMAXColumn = "Lorem ipsum dolor sit amet, consectetur adipisci elit, sed eiusmod tempor incidunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrum exercitationem ullam corporis suscipit laboriosam, nisi ut aliquid ex ea commodi consequatur. Quis aute iure reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint obcaecat cupiditat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.", xmlColumn = "<catalog><book id=\"bk101\"><author>Gambardella, Matthew</author><title>XML Developer's Guide</title><genre>Computer</genre><price>44.95</price><publish_date>2000-10-01</publish_date><description>An in-depth look at creating applications with XML.</description></book><book id=\"bk102\"><author>Ralls, Kim</author><title>Midnight Rain</title><genre>Fantasy</genre><price>5.95</price><publish_date>2000-12-16</publish_date><description>A former architect battles corporate zombies, an evil sorceress, and her own childhood to become queen of the world.</description></book><book id=\"bk103\"><author>Corets, Eva</author><title>Maeve Ascendant</title><genre>Fantasy</genre><price>5.95</price><publish_date>2000-11-17</publish_date><description>After the collapse of a nanotechnology society in England, the young survivors lay the foundation for a new society.</description></book><book id=\"bk104\"><author>Corets, Eva</author><title>Oberon's Legacy</title><genre>Fantasy</genre><price>5.95</price><publish_date>2001-03-10</publish_date><description>In post-apocalypse England, the mysterious agent known only as Oberon helps to create a new life for the inhabitants of London. Sequel to Maeve Ascendant.</description></book><book id=\"bk105\"><author>Corets, Eva</author><title>The Sundered Grail</title><genre>Fantasy</genre><price>5.95</price><publish_date>2001-09-10</publish_date><description>The two daughters of Maeve, half-sisters, battle one another for control of England. Sequel to Oberon's Legacy.</description></book><book id=\"bk106\"><author>Randall, Cynthia</author><title>Lover Birds</title><genre>Romance</genre><price>4.95</price><publish_date>2000-09-02</publish_date><description>When Carla meets Paul at an ornithology conference, tempers fly as feathers get ruffled.</description></book><book id=\"bk107\"><author>Thurman, Paula</author><title>Splish Splash</title><genre>Romance</genre><price>4.95</price><publish_date>2000-11-02</publish_date><description>A deep sea diver finds true love twenty thousand leagues beneath the sea.</description></book><book id=\"bk108\"><author>Knorr, Stefan</author><title>Creepy Crawlies</title><genre>Horror</genre><price>4.95</price><publish_date>2000-12-06</publish_date><description>An anthology of horror stories about roaches, centipedes, scorpions  and other insects.</description></book><book id=\"bk109\"><author>Kress, Peter</author><title>Paradox Lost</title><genre>Science Fiction</genre><price>6.95</price><publish_date>2000-11-02</publish_date><description>After an inadvertant trip through a Heisenberg Uncertainty Device, James Salway discovers the problems of being quantum.</description></book><book id=\"bk110\"><author>O'Brien, Tim</author><title>Microsoft .NET: The Programming Bible</title><genre>Computer</genre><price>36.95</price><publish_date>2000-12-09</publish_date><description>Microsoft's .NET initiative is explored in detail in this deep programmer's reference.</description></book><book id=\"bk111\"><author>O'Brien, Tim</author><title>MSXML3: A Comprehensive Guide</title><genre>Computer</genre><price>36.95</price><publish_date>2000-12-01</publish_date><description>The Microsoft MSXML3 parser is covered in detail, with attention to XML DOM interfaces, XSLT processing, SAX and more.</description></book><book id=\"bk112\"><author>Galos, Mike</author><title>Visual Studio 7: A Comprehensive Guide</title><genre>Computer</genre><price>49.95</price><publish_date>2001-04-16</publish_date><description>Microsoft Visual Studio 7 is explored in depth, looking at how Visual Basic, Visual C++, C#, and ASP+ are integrated into a comprehensive development environment.</description></book></catalog>" }, new Check_Model()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<Check_Model, Check_Model>(new Check_Model { varcharMAXColumn = "111", nvarcharMAXColumn = "new byte[] { 1, 2, 3, 4, 5, 6 }", xmlColumn = "<names><name>Christian Del Bianco</name></names>" }, new Check_Model()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<Check_Model, Check_Model>(new Check_Model { varcharMAXColumn = "111", nvarcharMAXColumn = "new byte[] { 1, 2, 3, 4, 5, 6 }", xmlColumn = "<names><name>Christian Del Bianco</name></names>" }, new Check_Model()));

            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([varcharMAXColumn], [nvarcharMAXColumn], [xmlColumn]) VALUES(@varcharMAXColumn, @nvarcharMAXColumn, @xml)";
                    sqlCommand.Parameters.AddWithValue("@varcharMAXColumn", _checkValues[ChangeType.Insert.ToString()].Item1.varcharMAXColumn);
                    sqlCommand.Parameters.AddWithValue("@nvarcharMAXColumn", _checkValues[ChangeType.Insert.ToString()].Item1.nvarcharMAXColumn);
                    sqlCommand.Parameters.Add(new SqlParameter("@xml", SqlDbType.Xml) {Value = ConvertString2SqlXml(_checkValues[ChangeType.Insert.ToString()].Item1.xmlColumn)});
                    sqlCommand.ExecuteNonQuery();
                }

                Thread.Sleep(1000);

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [varcharMAXColumn] = @varcharMAXColumn, [nvarcharMAXColumn] = @nvarcharMAXColumn, [xmlColumn] = @xml";
                    sqlCommand.Parameters.AddWithValue("@varcharMAXColumn", _checkValues[ChangeType.Update.ToString()].Item1.varcharMAXColumn);
                    sqlCommand.Parameters.AddWithValue("@nvarcharMAXColumn", _checkValues[ChangeType.Update.ToString()].Item1.nvarcharMAXColumn);
                    sqlCommand.Parameters.Add(new SqlParameter("@xml", SqlDbType.Xml) {Value = ConvertString2SqlXml(_checkValues[ChangeType.Update.ToString()].Item1.xmlColumn)});
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