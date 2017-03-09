using System;
using System.Collections.Generic;
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
    internal class Issue55Model
    {
        public decimal PaymentDiscount { get; set; }
        public int AllowQuantity { get; set; }
        public string DocNo { get; set; }
    }

    [TestClass]
    public class Issue55
    {
        private const string TableName = "BranchABC$Sales Invoice Header";
        private static readonly string ConnectionString = ConfigurationManager.ConnectionStrings["SqlServerConnectionString"].ConnectionString;
        private static int _counter;
        private static Dictionary<string, Tuple<Issue55Model, Issue55Model>> _checkValues = new Dictionary<string, Tuple<Issue55Model, Issue55Model>>();


        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('[{TableName}]', 'U') IS NOT NULL DROP TABLE [dbo].[{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Payment Discount %] decimal(18, 2), [Allow Quantity Disc_] int, [Applies-to Doc_ No_] [VARCHAR](100) NULL)";
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


        public TestContext TestContext { get; set; }


        [TestCategory("SqlServer")]
        [TestMethod]
        public void Issue27Tesst()
        {
            var mapper = new ModelToTableMapper<Issue55Model>();
            mapper.AddMapping(c => c.PaymentDiscount, "Payment Discount %");
            mapper.AddMapping(c => c.AllowQuantity, "Allow Quantity Disc_");
            mapper.AddMapping(c => c.DocNo, "Applies-to Doc_ No_");

            string objectNaming;
            var tableDependency = new SqlTableDependency<Issue55Model>(ConnectionString, TableName, mapper);

            try
            {
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                objectNaming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency.Dispose();
            }

            Assert.AreEqual(_counter, 3);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.DocNo, _checkValues[ChangeType.Insert.ToString()].Item1.DocNo);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.AllowQuantity, _checkValues[ChangeType.Insert.ToString()].Item1.AllowQuantity);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.PaymentDiscount, _checkValues[ChangeType.Insert.ToString()].Item1.PaymentDiscount);

            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.DocNo, _checkValues[ChangeType.Update.ToString()].Item1.DocNo);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.AllowQuantity, _checkValues[ChangeType.Update.ToString()].Item1.AllowQuantity);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.PaymentDiscount, _checkValues[ChangeType.Update.ToString()].Item1.PaymentDiscount);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.DocNo, _checkValues[ChangeType.Delete.ToString()].Item1.DocNo);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.AllowQuantity, _checkValues[ChangeType.Delete.ToString()].Item1.AllowQuantity);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.PaymentDiscount, _checkValues[ChangeType.Delete.ToString()].Item1.PaymentDiscount);

            Assert.IsTrue(SqlServerHelper.AreAllDbObjectDisposed(ConnectionString, objectNaming));
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<Issue55Model> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.DocNo = e.Entity.DocNo;
                    _checkValues[ChangeType.Insert.ToString()].Item2.AllowQuantity = e.Entity.AllowQuantity;
                    _checkValues[ChangeType.Insert.ToString()].Item2.PaymentDiscount = e.Entity.PaymentDiscount;
                    break;
                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.DocNo = e.Entity.DocNo;
                    _checkValues[ChangeType.Update.ToString()].Item2.AllowQuantity = e.Entity.AllowQuantity;
                    _checkValues[ChangeType.Update.ToString()].Item2.PaymentDiscount = e.Entity.PaymentDiscount;
                    break;
                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.DocNo = e.Entity.DocNo;
                    _checkValues[ChangeType.Delete.ToString()].Item2.AllowQuantity = e.Entity.AllowQuantity;
                    _checkValues[ChangeType.Delete.ToString()].Item2.PaymentDiscount = e.Entity.PaymentDiscount;
                    break;
            }
        }

        private static void ModifyTableContent()
        {
            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<Issue55Model, Issue55Model>(new Issue55Model { DocNo = "Christian", AllowQuantity = 1, PaymentDiscount = 9 }, new Issue55Model()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<Issue55Model, Issue55Model>(new Issue55Model { DocNo = "Velia", AllowQuantity = 2, PaymentDiscount = 3}, new Issue55Model()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<Issue55Model, Issue55Model>(new Issue55Model { DocNo = "Velia", AllowQuantity = 2, PaymentDiscount = 3 }, new Issue55Model()));

            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Payment Discount %], [Allow Quantity Disc_], [Applies-to Doc_ No_]) VALUES ({_checkValues[ChangeType.Insert.ToString()].Item1.PaymentDiscount}, {_checkValues[ChangeType.Insert.ToString()].Item1.AllowQuantity}, '{_checkValues[ChangeType.Insert.ToString()].Item1.DocNo}')";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(500);
                    
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Payment Discount %] = {_checkValues[ChangeType.Update.ToString()].Item1.PaymentDiscount}, [Allow Quantity Disc_] = {_checkValues[ChangeType.Update.ToString()].Item1.AllowQuantity}, [Applies-to Doc_ No_] = '{_checkValues[ChangeType.Update.ToString()].Item1.DocNo}'";
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