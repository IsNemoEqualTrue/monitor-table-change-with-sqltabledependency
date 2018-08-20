using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class Issue65Test : Base.SqlTableDependencyBaseTest
    {
        private class Issue65Model
        {
            public string Id { get; set; }

            [Column(TypeName = "Date")]
            public virtual DateTime InvoiceDate { get; set; }
        }

        private static readonly string TableName = typeof(Issue65Model).Name;
        private static readonly Dictionary<string, Tuple<Issue65Model, Issue65Model>> CheckValues = new Dictionary<string, Tuple<Issue65Model, Issue65Model>>();

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('[{TableName}]', 'U') IS NOT NULL DROP TABLE [dbo].[{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}] ([InvoiceDate] [DATE] NULL)";
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

            CheckValues.Clear();

            CheckValues.Add(ChangeType.Insert.ToString(), new Tuple<Issue65Model, Issue65Model>(new Issue65Model { InvoiceDate = DateTime.Now.AddDays(-51).Date }, new Issue65Model()));
            CheckValues.Add(ChangeType.Update.ToString(), new Tuple<Issue65Model, Issue65Model>(new Issue65Model { InvoiceDate = DateTime.Now.Date }, new Issue65Model()));
            CheckValues.Add(ChangeType.Delete.ToString(), new Tuple<Issue65Model, Issue65Model>(new Issue65Model { InvoiceDate = DateTime.Now.Date }, new Issue65Model()));
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
            SqlTableDependency<Issue65Model> tableDependency = null;

            try
            {
                tableDependency = new SqlTableDependency<Issue65Model>(ConnectionStringForTestUser);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.Start();

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(CheckValues[ChangeType.Insert.ToString()].Item2.InvoiceDate, CheckValues[ChangeType.Insert.ToString()].Item1.InvoiceDate);
            Assert.AreEqual(CheckValues[ChangeType.Update.ToString()].Item2.InvoiceDate, CheckValues[ChangeType.Update.ToString()].Item1.InvoiceDate);
            Assert.AreEqual(CheckValues[ChangeType.Delete.ToString()].Item2.InvoiceDate, CheckValues[ChangeType.Delete.ToString()].Item1.InvoiceDate);
        }
        private void TableDependency_Changed(object sender, RecordChangedEventArgs<Issue65Model> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues[ChangeType.Insert.ToString()].Item2.InvoiceDate = e.Entity.InvoiceDate;
                    break;

                case ChangeType.Update:
                    CheckValues[ChangeType.Update.ToString()].Item2.InvoiceDate = e.Entity.InvoiceDate;
                    break;

                case ChangeType.Delete:
                    CheckValues[ChangeType.Delete.ToString()].Item2.InvoiceDate = e.Entity.InvoiceDate;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([InvoiceDate]) VALUES(@dateColumn)";
                    sqlCommand.Parameters.Add(new SqlParameter("@dateColumn", SqlDbType.Date) { Value = CheckValues[ChangeType.Insert.ToString()].Item1.InvoiceDate });
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [InvoiceDate] = @dateColumn";
                    sqlCommand.Parameters.Add(new SqlParameter("@dateColumn", SqlDbType.Date) { Value = CheckValues[ChangeType.Update.ToString()].Item1.InvoiceDate });
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