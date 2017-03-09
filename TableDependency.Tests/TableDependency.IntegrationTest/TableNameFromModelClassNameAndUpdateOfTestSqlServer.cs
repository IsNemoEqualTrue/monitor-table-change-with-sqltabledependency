using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
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
    public class AAA_Item3
    {
        public long Id { get; set; }
        public string Name { get; set; }
        [Column(ColumnName)]
        public string FamilyName { get; set; }
        private const string ColumnName = "SURNAME";
        public static string GetColumnName => ColumnName;
    }

    [TestClass]
    public class TableNameFromModelClassNameAndUpdateOfTestSqlServer
    {
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServerConnectionString"].ConnectionString;
        private static readonly string TableName = typeof(AAA_Item3).Name.ToUpper();
        private static readonly Dictionary<string, Tuple<AAA_Item3, AAA_Item3>> CheckValues = new Dictionary<string, Tuple<AAA_Item3, AAA_Item3>>();
        private static int _counter = 0;

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

                    sqlCommand.CommandText =
                        $"CREATE TABLE [{TableName}]( " +
                        $"[Id] [int] IDENTITY(1, 1) NOT NULL, " +
                        $"[Name] [NVARCHAR](50) NULL, " +
                        $"[Surname] [NVARCHAR](MAX) NULL)";
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
        public void Test()
        {
            SqlTableDependency<AAA_Item3> tableDependency = null;
            string naming = null;

            try
            {
                UpdateOfModel<AAA_Item3> updateOF = new UpdateOfModel<AAA_Item3>();
                updateOF.Add(model => model.FamilyName);

                tableDependency = new SqlTableDependency<AAA_Item3>(ConnectionString, updateOf: updateOF);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(30000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 2);

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.FamilyName, CheckValues[ChangeType.Insert.ToString()].Item1.FamilyName);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.FamilyName, CheckValues[ChangeType.Delete.ToString()].Item1.FamilyName);

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, naming));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<AAA_Item3> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.Id = e.Entity.Id;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Insert.ToString()].Item2.FamilyName = e.Entity.FamilyName;
                    break;

                case ChangeType.Delete:
                   
                    CheckValues[ChangeType.Delete.ToString()].Item2.Id = e.Entity.Id;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Delete.ToString()].Item2.FamilyName = e.Entity.FamilyName;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<AAA_Item3, AAA_Item3>(new AAA_Item3 { Id = 23, Name = "Pizza Mergherita", FamilyName = "Pizza Mergherita" }, new AAA_Item3()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<AAA_Item3, AAA_Item3>(new AAA_Item3 { Id = 23, Name = "Pizza Funghi", FamilyName = "Pizza Mergherita" }, new AAA_Item3()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<AAA_Item3, AAA_Item3>(new AAA_Item3 { Id = 23, Name = "Pizza Funghi", FamilyName = "Pizza Mergherita" }, new AAA_Item3()));

            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name], [SURNAME]) VALUES ('{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', '{CheckValues[ChangeType.Insert.ToString()].Item1.FamilyName}')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = '{CheckValues[ChangeType.Update.ToString()].Item1.Name}'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);

                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);
                }
            }
        }
    }
}