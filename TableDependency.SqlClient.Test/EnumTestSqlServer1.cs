using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class EnumTestSqlServer1 : Base.SqlTableDependencyBaseTest
    {
        private enum TypeEnum1 : byte
        {
            Genitore = 1,
            Figlio = 2
        }

        private class EnumTestSqlServerModel1
        {
            public string Name { get; set; }
            public string Surname { get; set; }
            public TypeEnum1 Tipo { get; set; }
        }

        private static readonly string TableName = typeof(EnumTestSqlServerModel1).Name.ToUpper();
        private static int _counter;
        private static readonly Dictionary<string, Tuple<EnumTestSqlServerModel1, EnumTestSqlServerModel1>> CheckValues = new Dictionary<string, Tuple<EnumTestSqlServerModel1, EnumTestSqlServerModel1>>();

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}]";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Tipo] [TINYINT] NULL, [Name] [NVARCHAR](50) NULL, [Surname] [NVARCHAR](50) NULL)";
                    sqlCommand.ExecuteNonQuery();
                }
            }
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
            SqlTableDependency<EnumTestSqlServerModel1> tableDependency = null;

            try
            {
                tableDependency = new SqlTableDependency<EnumTestSqlServerModel1>(ConnectionStringForTestUser, TableName);
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Name, CheckValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Surname, CheckValues[ChangeType.Insert.ToString()].Item1.Surname);
            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.Tipo, CheckValues[ChangeType.Insert.ToString()].Item1.Tipo);

            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Name, CheckValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Surname, CheckValues[ChangeType.Update.ToString()].Item1.Surname);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.Tipo, CheckValues[ChangeType.Update.ToString()].Item1.Tipo);

            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Name, CheckValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Surname, CheckValues[ChangeType.Delete.ToString()].Item1.Surname);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.Tipo, CheckValues[ChangeType.Delete.ToString()].Item1.Tipo);

            Assert.IsTrue(base.AreAllDbObjectDisposed(tableDependency.DataBaseObjectsNamingConvention));
            Assert.IsTrue(base.CountConversationEndpoints(tableDependency.DataBaseObjectsNamingConvention) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<EnumTestSqlServerModel1> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Surname = e.Entity.Surname;
                    CheckValues[ChangeType.Insert.ToString()].Item2.Tipo = e.Entity.Tipo;
                    break;

                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Update.ToString()].Item2.Surname = e.Entity.Surname;
                    CheckValues[ChangeType.Update.ToString()].Item2.Tipo = e.Entity.Tipo;
                    break;

                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Surname = e.Entity.Surname;
                    CheckValues[ChangeType.Delete.ToString()].Item2.Tipo = e.Entity.Tipo;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<EnumTestSqlServerModel1, EnumTestSqlServerModel1>(new EnumTestSqlServerModel1 { Tipo = TypeEnum1.Figlio, Name = "Christian", Surname = "Del Bianco" }, new EnumTestSqlServerModel1()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<EnumTestSqlServerModel1, EnumTestSqlServerModel1>(new EnumTestSqlServerModel1 { Tipo = TypeEnum1.Genitore, Name = "Velia", Surname = "Del Bianco" }, new EnumTestSqlServerModel1()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<EnumTestSqlServerModel1, EnumTestSqlServerModel1>(new EnumTestSqlServerModel1 { Tipo = TypeEnum1.Genitore, Name = "Velia", Surname = "Del Bianco" }, new EnumTestSqlServerModel1()));

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Tipo], [Name], [Surname]) VALUES ({CheckValues[ChangeType.Insert.ToString()].Item1.Tipo.GetHashCode()}, N'{CheckValues[ChangeType.Insert.ToString()].Item1.Name}', N'{CheckValues[ChangeType.Insert.ToString()].Item1.Surname}')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = N'{CheckValues[ChangeType.Update.ToString()].Item1.Name}', [Tipo] = {CheckValues[ChangeType.Update.ToString()].Item1.Tipo.GetHashCode()}";
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