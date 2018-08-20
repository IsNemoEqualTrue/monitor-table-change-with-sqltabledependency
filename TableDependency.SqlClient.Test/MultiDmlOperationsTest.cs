using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.EventArgs;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class MultiDmlOperationsTest : Base.SqlTableDependencyBaseTest
    {
        private class MultiDmlOperationsTestSqlServerModel : IEquatable<MultiDmlOperationsTestSqlServerModel>
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public string Surname { get; set; }

            public bool Equals(MultiDmlOperationsTestSqlServerModel other)
            {
                if (other == null) return false;
                if (this.Name != other.Name) return false;
                if (this.Surname != other.Surname) return false;
                return true;
            }
        }

        private static readonly string TableName = typeof(MultiDmlOperationsTestSqlServerModel).Name;
        private static readonly List<MultiDmlOperationsTestSqlServerModel> ModifiedValues = new List<MultiDmlOperationsTestSqlServerModel>();
        private static readonly List<MultiDmlOperationsTestSqlServerModel> InitialValues = new List<MultiDmlOperationsTestSqlServerModel>();

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            InitialValues.Add(new MultiDmlOperationsTestSqlServerModel() { Name = "CHRISTIAN", Surname = "DEL BIANCO" });
            InitialValues.Add(new MultiDmlOperationsTestSqlServerModel() { Name = "VELIA", Surname = "CECCARELLI" });
            InitialValues.Add(new MultiDmlOperationsTestSqlServerModel() { Name = "ALFREDINA", Surname = "BRUSCHI" });

            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [int] IDENTITY(1, 1) NOT NULL, [First Name] [NVARCHAR](50) NOT NULL, [Second Name] [NVARCHAR](50) NOT NULL)";
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
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
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
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    foreach (var item in InitialValues)
                    {
                        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{item.Name}', '{item.Surname}')";
                        sqlCommand.ExecuteNonQuery();
                    }
                }
            }

            ModifiedValues.Clear();
            SqlTableDependency<MultiDmlOperationsTestSqlServerModel> tableDependency = null;

            try
            {
                var mapper = new ModelToTableMapper<MultiDmlOperationsTestSqlServerModel>();
                mapper.AddMapping(c => c.Name, "FIRST name");
                mapper.AddMapping(c => c.Surname, "Second Name");

                tableDependency = new SqlTableDependency<MultiDmlOperationsTestSqlServerModel>(ConnectionStringForTestUser, tableName: TableName, mapper: mapper);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.OnError += this.TableDependency_OnError;
                tableDependency.Start();

                var t = new Task(MultiDeleteOperation);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(3, ModifiedValues.Count);
            Assert.IsTrue(InitialValues.Any(i => i.Equals(ModifiedValues[0])));
            Assert.IsTrue(InitialValues.Any(i => i.Equals(ModifiedValues[1])));
            Assert.IsTrue(InitialValues.Any(i => i.Equals(ModifiedValues[2])));
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void TwoUpdateTest()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    foreach (var item in InitialValues)
                    {
                        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{item.Name}', '{item.Surname}')";
                        sqlCommand.ExecuteNonQuery();
                    }
                }
            }

            ModifiedValues.Clear();
            SqlTableDependency<MultiDmlOperationsTestSqlServerModel> tableDependency = null;

            try
            {
                var mapper = new ModelToTableMapper<MultiDmlOperationsTestSqlServerModel>();
                mapper.AddMapping(c => c.Name, "FIRST name");
                mapper.AddMapping(c => c.Surname, "Second Name");

                tableDependency = new SqlTableDependency<MultiDmlOperationsTestSqlServerModel>(ConnectionStringForTestUser, tableName: TableName, mapper: mapper);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.OnError += this.TableDependency_OnError;
                tableDependency.Start();

                Task.Factory.StartNew(() => MultiUpdateOperation("VELIA"));
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(2, ModifiedValues.Count);
            Assert.AreEqual(ModifiedValues[0].Name, "VELIA");
            Assert.AreEqual(ModifiedValues[1].Name, "VELIA");
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void ThreeUpdateTest()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    foreach (var item in InitialValues)
                    {
                        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{item.Name}', '{item.Surname}')";
                        sqlCommand.ExecuteNonQuery();
                    }
                }
            }

            ModifiedValues.Clear();
            SqlTableDependency<MultiDmlOperationsTestSqlServerModel> tableDependency = null;

            try
            {
                var mapper = new ModelToTableMapper<MultiDmlOperationsTestSqlServerModel>();
                mapper.AddMapping(c => c.Name, "FIRST name");
                mapper.AddMapping(c => c.Surname, "Second Name");

                tableDependency = new SqlTableDependency<MultiDmlOperationsTestSqlServerModel>(ConnectionStringForTestUser, tableName: TableName, mapper: mapper);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.OnError += this.TableDependency_OnError;
                tableDependency.Start();

                Task.Factory.StartNew(() => MultiUpdateOperation("xxx"));
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(3, ModifiedValues.Count);
            Assert.AreEqual(ModifiedValues[0].Name, "xxx");
            Assert.AreEqual(ModifiedValues[1].Name, "xxx");
            Assert.AreEqual(ModifiedValues[2].Name, "xxx");
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void MultiInsertTest()
        {
            ModifiedValues.Clear();
            SqlTableDependency<MultiDmlOperationsTestSqlServerModel> tableDependency = null;

            try
            {
                var mapper = new ModelToTableMapper<MultiDmlOperationsTestSqlServerModel>();
                mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second Name");

                tableDependency = new SqlTableDependency<MultiDmlOperationsTestSqlServerModel>(ConnectionStringForTestUser, tableName: TableName, mapper: mapper);
                tableDependency.OnChanged += this.TableDependency_Changed;
                tableDependency.OnError += this.TableDependency_OnError;
                tableDependency.Start();

                var t = new Task(MultiInsertOperation);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(3, ModifiedValues.Count);
            Assert.IsTrue(InitialValues.Any(i => i.Equals(ModifiedValues[0])));
            Assert.IsTrue(InitialValues.Any(i => i.Equals(ModifiedValues[1])));
            Assert.IsTrue(InitialValues.Any(i => i.Equals(ModifiedValues[2])));
        }

        private void TableDependency_OnError(object sender, ErrorEventArgs e)
        {
            throw e.Error;
        }

        private void TableDependency_Changed(object sender, RecordChangedEventArgs<MultiDmlOperationsTestSqlServerModel> e)
        {
            ModifiedValues.Add(e.Entity);
        }

        private static void MultiInsertOperation()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    foreach (var item in InitialValues)
                    {
                        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{item.Name}', '{item.Surname}')";
                        sqlCommand.ExecuteNonQuery();
                    }
                }
            }

            Thread.Sleep(500);
        }

        private static void MultiDeleteOperation()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }

            Thread.Sleep(500);
        }

        private static void MultiUpdateOperation(string value)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = '{value}'";
                    sqlCommand.ExecuteNonQuery();
                }
            }

            Thread.Sleep(500);
        }
    }
}