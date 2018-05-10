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
using TableDependency.SqlClient.BaseTests;

namespace TableDependency.SqlClient.IntegrationTests
{
    public class ComputedColumnModel
    {
        public string Name { get; set; }

        public DateTime BirthDate { get; set; }

        [Column("Age")]
        public int CalculatedAge { get; set; }
    }

    [TestClass]
    public class ComputedColumnTest : SqlTableDependencyBaseTest
    {
        private static readonly string TableName = typeof(ComputedColumnModel).Name;
        private static int _counter;
        private static Dictionary<string, Tuple<ComputedColumnModel, ComputedColumnModel>> _checkValues = new Dictionary<string, Tuple<ComputedColumnModel, ComputedColumnModel>>();
        private static Dictionary<string, Tuple<ComputedColumnModel, ComputedColumnModel>> _checkValuesOld = new Dictionary<string, Tuple<ComputedColumnModel, ComputedColumnModel>>();

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

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Name] [NVARCHAR](50) NULL, [BirthDate] [DATETIME] NULL)";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"ALTER TABLE [{TableName}] ADD [Age] AS DATEDIFF(YEAR, [BirthDate], GETDATE())";
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

            _counter = 0;

            _checkValues.Add(ChangeType.Insert.ToString(), new Tuple<ComputedColumnModel, ComputedColumnModel>(new ComputedColumnModel { Name = "Christian", BirthDate = DateTime.Now.AddYears(-46).Date, CalculatedAge = 46 }, new ComputedColumnModel()));
            _checkValues.Add(ChangeType.Update.ToString(), new Tuple<ComputedColumnModel, ComputedColumnModel>(new ComputedColumnModel { Name = "Nonna Velia", BirthDate = DateTime.Now.AddYears(-95).Date, CalculatedAge = 95 }, new ComputedColumnModel()));
            _checkValues.Add(ChangeType.Delete.ToString(), new Tuple<ComputedColumnModel, ComputedColumnModel>(new ComputedColumnModel { Name = "Nonna Velia", BirthDate = DateTime.Now.AddYears(-95).Date, CalculatedAge = 95 }, new ComputedColumnModel()));

            _checkValuesOld.Add(ChangeType.Insert.ToString(), new Tuple<ComputedColumnModel, ComputedColumnModel>(new ComputedColumnModel { Name = "Christian", BirthDate = DateTime.Now.AddYears(-46).Date, CalculatedAge = 46 }, new ComputedColumnModel()));
            _checkValuesOld.Add(ChangeType.Update.ToString(), new Tuple<ComputedColumnModel, ComputedColumnModel>(new ComputedColumnModel { Name = "Nonna Velia", BirthDate = DateTime.Now.AddYears(-95).Date, CalculatedAge = 95 }, new ComputedColumnModel()));
            _checkValuesOld.Add(ChangeType.Delete.ToString(), new Tuple<ComputedColumnModel, ComputedColumnModel>(new ComputedColumnModel { Name = "Nonna Velia", BirthDate = DateTime.Now.AddYears(-95).Date, CalculatedAge = 95 }, new ComputedColumnModel()));
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
        public void TestWithOldValues()
        {
            SqlTableDependency<ComputedColumnModel> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<ComputedColumnModel>(ConnectionStringForTestUser, includeOldValues: true);
                naming = tableDependency.DataBaseObjectsNamingConvention;
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 5 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Name, _checkValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.BirthDate, _checkValues[ChangeType.Insert.ToString()].Item1.BirthDate);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.CalculatedAge, _checkValues[ChangeType.Insert.ToString()].Item1.CalculatedAge);
            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Name, _checkValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.BirthDate, _checkValues[ChangeType.Update.ToString()].Item1.BirthDate);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.CalculatedAge, _checkValues[ChangeType.Update.ToString()].Item1.CalculatedAge);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.Name, _checkValues[ChangeType.Insert.ToString()].Item2.Name);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.BirthDate, _checkValues[ChangeType.Insert.ToString()].Item2.BirthDate);
            Assert.AreEqual(_checkValuesOld[ChangeType.Update.ToString()].Item2.CalculatedAge, _checkValues[ChangeType.Insert.ToString()].Item2.CalculatedAge);
            
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Name, _checkValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.BirthDate, _checkValues[ChangeType.Delete.ToString()].Item1.BirthDate);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.CalculatedAge, _checkValues[ChangeType.Delete.ToString()].Item1.CalculatedAge);
            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void Test()
        {
            SqlTableDependency<ComputedColumnModel> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<ComputedColumnModel>(ConnectionStringForTestUser);
                naming = tableDependency.DataBaseObjectsNamingConvention;
                tableDependency.OnChanged += TableDependency_Changed;
                tableDependency.Start();

                var t = new Task(ModifyTableContent);
                t.Start();
                Thread.Sleep(1000 * 5 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.Name, _checkValues[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.BirthDate, _checkValues[ChangeType.Insert.ToString()].Item1.BirthDate);
            Assert.AreEqual(_checkValues[ChangeType.Insert.ToString()].Item2.CalculatedAge, _checkValues[ChangeType.Insert.ToString()].Item1.CalculatedAge);
            Assert.IsNull(_checkValuesOld[ChangeType.Insert.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.Name, _checkValues[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.BirthDate, _checkValues[ChangeType.Update.ToString()].Item1.BirthDate);
            Assert.AreEqual(_checkValues[ChangeType.Update.ToString()].Item2.CalculatedAge, _checkValues[ChangeType.Update.ToString()].Item1.CalculatedAge);
            Assert.IsNull(_checkValuesOld[ChangeType.Update.ToString()]);

            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.Name, _checkValues[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.BirthDate, _checkValues[ChangeType.Delete.ToString()].Item1.BirthDate);
            Assert.AreEqual(_checkValues[ChangeType.Delete.ToString()].Item2.CalculatedAge, _checkValues[ChangeType.Delete.ToString()].Item1.CalculatedAge);
            Assert.IsNull(_checkValuesOld[ChangeType.Delete.ToString()]);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private static void TableDependency_Changed(object sender, RecordChangedEventArgs<ComputedColumnModel> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    _checkValues[ChangeType.Insert.ToString()].Item2.BirthDate = e.Entity.BirthDate;
                    _checkValues[ChangeType.Insert.ToString()].Item2.CalculatedAge = e.Entity.CalculatedAge;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.Name = e.EntityOldValues.Name;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.BirthDate = e.EntityOldValues.BirthDate;
                        _checkValuesOld[ChangeType.Insert.ToString()].Item2.CalculatedAge = e.EntityOldValues.CalculatedAge;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Insert.ToString()] = null;
                    }

                    break;

                case ChangeType.Update:
                    _checkValues[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                    _checkValues[ChangeType.Update.ToString()].Item2.BirthDate = e.Entity.BirthDate;
                    _checkValues[ChangeType.Update.ToString()].Item2.CalculatedAge = e.Entity.CalculatedAge;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.Name = e.EntityOldValues.Name;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.BirthDate = e.EntityOldValues.BirthDate;
                        _checkValuesOld[ChangeType.Update.ToString()].Item2.CalculatedAge = e.EntityOldValues.CalculatedAge;
                    }
                    else
                    {
                        _checkValuesOld[ChangeType.Update.ToString()] = null;
                    }

                    break; 

                case ChangeType.Delete:
                    _checkValues[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    _checkValues[ChangeType.Delete.ToString()].Item2.BirthDate = e.Entity.BirthDate;
                    _checkValues[ChangeType.Delete.ToString()].Item2.CalculatedAge = e.Entity.CalculatedAge;

                    if (e.EntityOldValues != null)
                    {
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.Name = e.EntityOldValues.Name;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.BirthDate = e.EntityOldValues.BirthDate;
                        _checkValuesOld[ChangeType.Delete.ToString()].Item2.CalculatedAge = e.EntityOldValues.CalculatedAge;
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
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name], [BirthDate]) VALUES (@name, @birth)";
                    sqlCommand.Parameters.Add(new SqlParameter("@name", SqlDbType.VarChar) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.Name });
                    sqlCommand.Parameters.Add(new SqlParameter("@birth", SqlDbType.Date) { Value = _checkValues[ChangeType.Insert.ToString()].Item1.BirthDate });
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = @name, [BirthDate] = @birth";
                    sqlCommand.Parameters.Add(new SqlParameter("@name", SqlDbType.VarChar) { Value = _checkValues[ChangeType.Update.ToString()].Item1.Name });
                    sqlCommand.Parameters.Add(new SqlParameter("@birth", SqlDbType.Date) { Value = _checkValues[ChangeType.Update.ToString()].Item1.BirthDate });
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