using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Base;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class Issue55Test : Base.SqlTableDependencyBaseTest
    {
        private class Issue55Model
        {
            public decimal PaymentDiscount { get; set; }
            public int AllowQuantity { get; set; }
            public string DocNo { get; set; }
        }

        private const string TableName = "BranchABC$Sales Invoice Header";
        private static int _counter;
        private static Dictionary<string, Tuple<Issue55Model, Issue55Model>> _checkValues = new Dictionary<string, Tuple<Issue55Model, Issue55Model>>();
        private static Dictionary<string, Tuple<Issue55Model, Issue55Model>> _checkValuesOld = new Dictionary<string, Tuple<Issue55Model, Issue55Model>>();

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('[{TableName}]', 'U') IS NOT NULL DROP TABLE [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Payment Discount %] decimal(18, 2), [Allow Quantity Disc_] int, [Applies-to Doc_ No_] [VARCHAR](100) NULL)";
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

            _counter = 0;

            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<Issue55Model, Issue55Model>(new Issue55Model { DocNo = "Christian", AllowQuantity = 1, PaymentDiscount = 9 }, new Issue55Model()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<Issue55Model, Issue55Model>(new Issue55Model { DocNo = "Velia", AllowQuantity = 2, PaymentDiscount = 3 }, new Issue55Model()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<Issue55Model, Issue55Model>(new Issue55Model { DocNo = "Velia", AllowQuantity = 2, PaymentDiscount = 3 }, new Issue55Model()));

            _checkValuesOld.Add(ChangeType.Insert.ToString(), new Tuple<Issue55Model, Issue55Model>(new Issue55Model { DocNo = "Christian", AllowQuantity = 1, PaymentDiscount = 9 }, new Issue55Model()));
            _checkValuesOld.Add(ChangeType.Update.ToString(), new Tuple<Issue55Model, Issue55Model>(new Issue55Model { DocNo = "Velia", AllowQuantity = 2, PaymentDiscount = 3 }, new Issue55Model()));
            _checkValuesOld.Add(ChangeType.Delete.ToString(), new Tuple<Issue55Model, Issue55Model>(new Issue55Model { DocNo = "Velia", AllowQuantity = 2, PaymentDiscount = 3 }, new Issue55Model()));
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
            var mapper = new ModelToTableMapper<Issue55Model>();
            mapper.AddMapping(c => c.PaymentDiscount, "Payment Discount %");
            mapper.AddMapping(c => c.AllowQuantity, "Allow Quantity Disc_");
            mapper.AddMapping(c => c.DocNo, "Applies-to Doc_ No_");

            string objectNaming;
            var tableDependency = new SqlTableDependency<Issue55Model>(
                ConnectionStringForTestUser, 
                includeOldValues: false, 
                tableName: TableName, 
                mapper: mapper);

            try
            {
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                objectNaming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency.Dispose();
            }

            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.DocNo, _checkValues[ChangeType.Insert.ToString()].Item1.DocNo);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.AllowQuantity, _checkValues[ChangeType.Insert.ToString()].Item1.AllowQuantity);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.PaymentDiscount, _checkValues[ChangeType.Insert.ToString()].Item1.PaymentDiscount);
            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.DocNo, _checkValues[ChangeType.Update.ToString()].Item1.DocNo);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.AllowQuantity, _checkValues[ChangeType.Update.ToString()].Item1.AllowQuantity);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.PaymentDiscount, _checkValues[ChangeType.Update.ToString()].Item1.PaymentDiscount);
            Assert.IsNull(_checkValuesOld[ChangeType.Update.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.DocNo, _checkValues[ChangeType.Delete.ToString()].Item1.DocNo);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.AllowQuantity, _checkValues[ChangeType.Delete.ToString()].Item1.AllowQuantity);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.PaymentDiscount, _checkValues[ChangeType.Delete.ToString()].Item1.PaymentDiscount);
            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(objectNaming));
            Assert.IsTrue(base.CountConversationEndpoints(objectNaming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TestWithOldValues()
        {
            var mapper = new ModelToTableMapper<Issue55Model>();
            mapper.AddMapping(c => c.PaymentDiscount, "Payment Discount %");
            mapper.AddMapping(c => c.AllowQuantity, "Allow Quantity Disc_");
            mapper.AddMapping(c => c.DocNo, "Applies-to Doc_ No_");

            string objectNaming;
            var tableDependency = new SqlTableDependency<Issue55Model>(
                ConnectionStringForTestUser, 
                includeOldValues: true, 
                tableName: TableName, 
                mapper: mapper);

            try
            {
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();
                objectNaming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency.Dispose();
            }

            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.DocNo, _checkValues[ChangeType.Insert.ToString()].Item1.DocNo);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.AllowQuantity, _checkValues[ChangeType.Insert.ToString()].Item1.AllowQuantity);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.PaymentDiscount, _checkValues[ChangeType.Insert.ToString()].Item1.PaymentDiscount);
            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.DocNo, _checkValues[ChangeType.Update.ToString()].Item1.DocNo);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.AllowQuantity, _checkValues[ChangeType.Update.ToString()].Item1.AllowQuantity);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.PaymentDiscount, _checkValues[ChangeType.Update.ToString()].Item1.PaymentDiscount);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.DocNo, _checkValues[ChangeType.Insert.ToString()].Item2.DocNo);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.AllowQuantity, _checkValues[ChangeType.Insert.ToString()].Item2.AllowQuantity);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.PaymentDiscount, _checkValues[ChangeType.Insert.ToString()].Item2.PaymentDiscount);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.DocNo, _checkValues[ChangeType.Delete.ToString()].Item1.DocNo);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.AllowQuantity, _checkValues[ChangeType.Delete.ToString()].Item1.AllowQuantity);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.PaymentDiscount, _checkValues[ChangeType.Delete.ToString()].Item1.PaymentDiscount);
            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(objectNaming));
            Assert.IsTrue(base.CountConversationEndpoints(objectNaming) == 0);
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

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.DocNo = e.EntityOldValues.DocNo;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.AllowQuantity = e.EntityOldValues.AllowQuantity;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.PaymentDiscount = e.EntityOldValues.PaymentDiscount;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()] = null;
                    }

                    break;

                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.DocNo = e.Entity.DocNo;
                    _checkValues[ChangeType.Update.ToString()].Item2.AllowQuantity = e.Entity.AllowQuantity;
                    _checkValues[ChangeType.Update.ToString()].Item2.PaymentDiscount = e.Entity.PaymentDiscount;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.DocNo = e.EntityOldValues.DocNo;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.AllowQuantity = e.EntityOldValues.AllowQuantity;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.PaymentDiscount = e.EntityOldValues.PaymentDiscount;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Update.ToString()] = null;
                    }

                    break;


                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.DocNo = e.Entity.DocNo;
                    _checkValues[ChangeType.Delete.ToString()].Item2.AllowQuantity = e.Entity.AllowQuantity;
                    _checkValues[ChangeType.Delete.ToString()].Item2.PaymentDiscount = e.Entity.PaymentDiscount;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.DocNo = e.EntityOldValues.DocNo;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.AllowQuantity = e.EntityOldValues.AllowQuantity;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.PaymentDiscount = e.EntityOldValues.PaymentDiscount;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Delete.ToString()] = null;
                    }

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
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Payment Discount %], [Allow Quantity Disc_], [Applies-to Doc_ No_]) VALUES ({_checkValues[ChangeType.Insert.ToString()].Item1.PaymentDiscount}, {_checkValues[ChangeType.Insert.ToString()].Item1.AllowQuantity}, '{_checkValues[ChangeType.Insert.ToString()].Item1.DocNo}')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Payment Discount %] = {_checkValues[ChangeType.Update.ToString()].Item1.PaymentDiscount}, [Allow Quantity Disc_] = {_checkValues[ChangeType.Update.ToString()].Item1.AllowQuantity}, [Applies-to Doc_ No_] = '{_checkValues[ChangeType.Update.ToString()].Item1.DocNo}'";
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