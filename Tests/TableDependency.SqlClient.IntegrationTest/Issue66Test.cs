using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Abstracts;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient.BaseTests;
using TableDependency.SqlClient.Where;

namespace TableDependency.SqlClient.IntegrationTests
{
    [TestClass]
    public class Issue66Test : SqlTableDependencyBaseTest
    {
        private class Issue66Model1
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public string City { get; set; }
        }

        private class Issue66Model2 : Issue66Model1
        {
            public string Surname { get; set; }
        }

        private static readonly string TableName = "Issue66Model";
        private static readonly Dictionary<string, List<Issue66Model1>> CheckValues1 = new Dictionary<string, List<Issue66Model1>>();
        private static readonly Dictionary<string, List<Issue66Model1>> CheckValuesOld1 = new Dictionary<string, List<Issue66Model1>>();
        private static readonly Dictionary<string, List<Issue66Model2>> CheckValues2 = new Dictionary<string, List<Issue66Model2>>();
        private static readonly Dictionary<string, List<Issue66Model2>> CheckValuesOld2 = new Dictionary<string, List<Issue66Model2>>();

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('[{TableName}1]', 'U') IS NOT NULL DROP TABLE [dbo].[{TableName}1]";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}1] ([Id] [INT] NULL, [Name] [NVARCHAR](50) NULL, [City] [NVARCHAR](50) NULL)";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"IF OBJECT_ID('[{TableName}2]', 'U') IS NOT NULL DROP TABLE [dbo].[{TableName}2]";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"CREATE TABLE [{TableName}2] ([Id] [INT] NULL, [Name] [NVARCHAR](50) NULL, [Second Name] [NVARCHAR](50) NULL, [City] [NVARCHAR](50) NULL)";
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
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}1];";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}2];";
                    sqlCommand.ExecuteNonQuery();
                }
            }

            CheckValues1.Clear();
            CheckValuesOld1.Clear();

            CheckValues1.Add(ChangeType.Insert.ToString(), new List<Issue66Model1>());
            CheckValues1.Add(ChangeType.Update.ToString(), new List<Issue66Model1>());
            CheckValues1.Add(ChangeType.Delete.ToString(), new List<Issue66Model1>());
            CheckValuesOld1.Add(ChangeType.Insert.ToString(), new List<Issue66Model1>());
            CheckValuesOld1.Add(ChangeType.Update.ToString(), new List<Issue66Model1>());
            CheckValuesOld1.Add(ChangeType.Delete.ToString(), new List<Issue66Model1>());

            CheckValues2.Clear();
            CheckValuesOld2.Clear();

            CheckValues2.Add(ChangeType.Insert.ToString(), new List<Issue66Model2>());
            CheckValues2.Add(ChangeType.Update.ToString(), new List<Issue66Model2>());
            CheckValues2.Add(ChangeType.Delete.ToString(), new List<Issue66Model2>());
            CheckValuesOld2.Add(ChangeType.Insert.ToString(), new List<Issue66Model2>());
            CheckValuesOld2.Add(ChangeType.Update.ToString(), new List<Issue66Model2>());
            CheckValuesOld2.Add(ChangeType.Delete.ToString(), new List<Issue66Model2>());
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}1', 'U') IS NOT NULL DROP TABLE [{TableName}1];";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}2', 'U') IS NOT NULL DROP TABLE [{TableName}2];";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void Test1()
        {
            SqlTableDependency<Issue66Model1> tableDependency = null;
            string naming;

            try
            {
                tableDependency = new SqlTableDependency<Issue66Model1>(ConnectionStringForTestUser, includeOldValues: true, tableName: TableName + "1");
                tableDependency.OnChanged += this.TableDependency_Changed1;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent1);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(CheckValues1[ChangeType.Insert.ToString()][0].Id, 1);
            Assert.AreEqual(CheckValues1[ChangeType.Insert.ToString()][0].Name, "CHRISTIAN");
            Assert.AreEqual(CheckValues1[ChangeType.Insert.ToString()][0].City, "LAVENA PONTE TRESA");
            Assert.AreEqual(CheckValues1[ChangeType.Insert.ToString()][1].Id, 2);
            Assert.AreEqual(CheckValues1[ChangeType.Insert.ToString()][1].Name, "VALENTINA");
            Assert.AreEqual(CheckValues1[ChangeType.Insert.ToString()][1].City, "LAVENA PONTE TRESA");

            Assert.AreEqual(CheckValues1[ChangeType.Update.ToString()][0].City, "BAAR");
            Assert.AreEqual(CheckValues1[ChangeType.Update.ToString()][1].City, "BAAR");
            Assert.AreEqual(CheckValuesOld1[ChangeType.Update.ToString()][0].City, "LAVENA PONTE TRESA");
            Assert.AreEqual(CheckValuesOld1[ChangeType.Update.ToString()][1].City, "LAVENA PONTE TRESA");

            Assert.AreEqual(CheckValues1[ChangeType.Delete.ToString()][0].Name, "christian");
            Assert.AreEqual(CheckValues1[ChangeType.Delete.ToString()][0].City, "BAAR");
            Assert.AreEqual(CheckValues1[ChangeType.Delete.ToString()][1].Name, "valentina");
            Assert.AreEqual(CheckValues1[ChangeType.Delete.ToString()][1].City, "BAAR");

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void Test2()
        {
            SqlTableDependency<Issue66Model2> tableDependency = null;
            string naming;

            var mapper = new ModelToTableMapper<Issue66Model2>();
            mapper.AddMapping(c => c.Surname, "Second Name");

            var updateOf = new UpdateOfModel<Issue66Model2>();
            updateOf.Add(i => i.Surname);
            updateOf.Add(i => i.City);

            try
            {
                tableDependency = new SqlTableDependency<Issue66Model2>(ConnectionStringForTestUser, includeOldValues: true, tableName: TableName + "2", mapper: mapper, updateOf: updateOf);
                tableDependency.OnChanged += this.TableDependency_Changed2;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent2);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(CheckValues2[ChangeType.Insert.ToString()][0].Id, 1);
            Assert.AreEqual(CheckValues2[ChangeType.Insert.ToString()][0].Name, "CHRISTIAN");
            Assert.AreEqual(CheckValues2[ChangeType.Insert.ToString()][0].City, "LAVENA PONTE TRESA");
            Assert.AreEqual(CheckValues2[ChangeType.Insert.ToString()][1].Id, 2);
            Assert.AreEqual(CheckValues2[ChangeType.Insert.ToString()][1].Name, "VALENTINA");
            Assert.AreEqual(CheckValues2[ChangeType.Insert.ToString()][1].City, "LAVENA PONTE TRESA");

            Assert.AreEqual(CheckValues2[ChangeType.Update.ToString()][0].City, "BAAR");
            Assert.AreEqual(CheckValues2[ChangeType.Update.ToString()][1].City, "BAAR");
            Assert.AreEqual(CheckValuesOld2[ChangeType.Update.ToString()][0].City, "LAVENA PONTE TRESA");
            Assert.AreEqual(CheckValuesOld2[ChangeType.Update.ToString()][1].City, "LAVENA PONTE TRESA");

            Assert.AreEqual(CheckValues2[ChangeType.Delete.ToString()][0].Surname, "del bianco");
            Assert.AreEqual(CheckValues2[ChangeType.Delete.ToString()][0].Name, "christian");
            Assert.AreEqual(CheckValues2[ChangeType.Delete.ToString()][0].City, "BAAR");
            Assert.AreEqual(CheckValues2[ChangeType.Delete.ToString()][1].Surname, "del bianco");
            Assert.AreEqual(CheckValues2[ChangeType.Delete.ToString()][1].Name, "valentina");
            Assert.AreEqual(CheckValues2[ChangeType.Delete.ToString()][1].City, "BAAR");

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void Test3()
        {
            SqlTableDependency<Issue66Model2> tableDependency = null;
            string naming;

            var mapper = new ModelToTableMapper<Issue66Model2>();
            mapper.AddMapping(c => c.Surname, "Second Name");

            try
            {
                tableDependency = new SqlTableDependency<Issue66Model2>(ConnectionStringForTestUser, includeOldValues: true, tableName: TableName + "2", mapper: mapper);
                tableDependency.OnChanged += this.TableDependency_Changed2;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent2);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(CheckValues2[ChangeType.Insert.ToString()][0].Id, 1);
            Assert.AreEqual(CheckValues2[ChangeType.Insert.ToString()][0].Name, "CHRISTIAN");
            Assert.AreEqual(CheckValues2[ChangeType.Insert.ToString()][0].City, "LAVENA PONTE TRESA");
            Assert.AreEqual(CheckValues2[ChangeType.Insert.ToString()][1].Id, 2);
            Assert.AreEqual(CheckValues2[ChangeType.Insert.ToString()][1].Name, "VALENTINA");
            Assert.AreEqual(CheckValues2[ChangeType.Insert.ToString()][1].City, "LAVENA PONTE TRESA");

            Assert.AreEqual(CheckValues2[ChangeType.Update.ToString()][0].City, "BAAR");
            Assert.AreEqual(CheckValues2[ChangeType.Update.ToString()][1].City, "BAAR");
            Assert.AreEqual(CheckValuesOld2[ChangeType.Update.ToString()][0].City, "LAVENA PONTE TRESA");
            Assert.AreEqual(CheckValuesOld2[ChangeType.Update.ToString()][1].City, "LAVENA PONTE TRESA");

            Assert.AreEqual(CheckValues2[ChangeType.Delete.ToString()][0].Surname, "del bianco");
            Assert.AreEqual(CheckValues2[ChangeType.Delete.ToString()][0].Name, "christian");
            Assert.AreEqual(CheckValues2[ChangeType.Delete.ToString()][0].City, "BAAR");
            Assert.AreEqual(CheckValues2[ChangeType.Delete.ToString()][1].Surname, "del bianco");
            Assert.AreEqual(CheckValues2[ChangeType.Delete.ToString()][1].Name, "valentina");
            Assert.AreEqual(CheckValues2[ChangeType.Delete.ToString()][1].City, "BAAR");

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void Test4()
        {
            SqlTableDependency<Issue66Model1> tableDependency = null;
            string naming;

            var updateOf = new UpdateOfModel<Issue66Model1>();
            updateOf.Add(i => i.Name);
            updateOf.Add(i => i.City);

            try
            {
                tableDependency = new SqlTableDependency<Issue66Model1>(ConnectionStringForTestUser, includeOldValues: true, tableName: TableName + "1", updateOf: updateOf);
                tableDependency.OnChanged += this.TableDependency_Changed1;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent1);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(CheckValues1[ChangeType.Insert.ToString()][0].Id, 1);
            Assert.AreEqual(CheckValues1[ChangeType.Insert.ToString()][0].Name, "CHRISTIAN");
            Assert.AreEqual(CheckValues1[ChangeType.Insert.ToString()][0].City, "LAVENA PONTE TRESA");
            Assert.AreEqual(CheckValues1[ChangeType.Insert.ToString()][1].Id, 2);
            Assert.AreEqual(CheckValues1[ChangeType.Insert.ToString()][1].Name, "VALENTINA");
            Assert.AreEqual(CheckValues1[ChangeType.Insert.ToString()][1].City, "LAVENA PONTE TRESA");

            Assert.AreEqual(CheckValues1[ChangeType.Update.ToString()][0].City, "BAAR");
            Assert.AreEqual(CheckValues1[ChangeType.Update.ToString()][1].City, "BAAR");
            Assert.AreEqual(CheckValuesOld1[ChangeType.Update.ToString()][0].City, "LAVENA PONTE TRESA");
            Assert.AreEqual(CheckValuesOld1[ChangeType.Update.ToString()][1].City, "LAVENA PONTE TRESA");

            Assert.AreEqual(CheckValues1[ChangeType.Delete.ToString()][0].Name, "christian");
            Assert.AreEqual(CheckValues1[ChangeType.Delete.ToString()][0].City, "BAAR");
            Assert.AreEqual(CheckValues1[ChangeType.Delete.ToString()][1].Name, "valentina");
            Assert.AreEqual(CheckValues1[ChangeType.Delete.ToString()][1].City, "BAAR");

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void Test5()
        {
            SqlTableDependency<Issue66Model2> tableDependency = null;
            string naming;

            var mapper = new ModelToTableMapper<Issue66Model2>();
            mapper.AddMapping(c => c.Surname, "Second Name");

            Expression<Func<Issue66Model2, bool>> expression = p => p.Id == 1 && p.Surname == "DEL BIANCO";
            ITableDependencyFilter whereCondition = new SqlTableDependencyFilter<Issue66Model2>(expression, mapper);

            var updateOf = new UpdateOfModel<Issue66Model2>();
            updateOf.Add(i => i.Surname);
            updateOf.Add(i => i.City);

            try
            {
                tableDependency = new SqlTableDependency<Issue66Model2>(
                    ConnectionStringForTestUser, 
                    includeOldValues: true, 
                    tableName: TableName + "2", 
                    mapper: mapper, 
                    updateOf: updateOf,
                    filter: whereCondition);

                tableDependency.OnChanged += this.TableDependency_Changed2;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent5);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(CheckValues2[ChangeType.Insert.ToString()].Count, 0);

            Assert.AreEqual(CheckValues2[ChangeType.Update.ToString()].Count, 1);

            Assert.AreEqual(CheckValues2[ChangeType.Update.ToString()][0].Id, 1);
            Assert.AreEqual(CheckValues2[ChangeType.Update.ToString()][0].Surname, "DEL BIANCO");
            Assert.AreEqual(CheckValues2[ChangeType.Update.ToString()][0].Name, "CHRISTIAN");
            Assert.AreEqual(CheckValues2[ChangeType.Update.ToString()][0].City, "BAAR");
            Assert.AreEqual(CheckValuesOld2[ChangeType.Update.ToString()][0].Id, 1);
            Assert.AreEqual(CheckValuesOld2[ChangeType.Update.ToString()][0].Surname, "DELBIANCO");
            Assert.AreEqual(CheckValuesOld2[ChangeType.Update.ToString()][0].Name, "CHRISTIAN");
            Assert.AreEqual(CheckValuesOld2[ChangeType.Update.ToString()][0].City, "LAVENA PONTE TRESA");

            Assert.AreEqual(CheckValues2[ChangeType.Delete.ToString()].Count, 1);

            Assert.AreEqual(CheckValues2[ChangeType.Delete.ToString()][0].Surname, "DEL BIANCO");
            Assert.AreEqual(CheckValues2[ChangeType.Delete.ToString()][0].Name, "christian");
            Assert.AreEqual(CheckValues2[ChangeType.Delete.ToString()][0].City, "BAAR");

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void Test6()
        {
            SqlTableDependency<Issue66Model2> tableDependency = null;
            string naming;

            var mapper = new ModelToTableMapper<Issue66Model2>();
            mapper.AddMapping(c => c.Surname, "Second Name");

            Expression<Func<Issue66Model2, bool>> expression = p => p.Id == 1 && p.Surname == "DEL BIANCO";
            ITableDependencyFilter whereCondition = new SqlTableDependencyFilter<Issue66Model2>(expression, mapper);

            var updateOf = new UpdateOfModel<Issue66Model2>();
            updateOf.Add(i => i.Surname);
            updateOf.Add(i => i.City);

            try
            {
                tableDependency = new SqlTableDependency<Issue66Model2>(
                    ConnectionStringForTestUser,
                    includeOldValues: true,
                    tableName: TableName + "2",
                    mapper: mapper,
                    updateOf: updateOf,
                    notifyOn: DmlTriggerType.Update,
                    filter: whereCondition);

                tableDependency.OnChanged += this.TableDependency_Changed2;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent5);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(CheckValues2[ChangeType.Insert.ToString()].Count, 0);

            Assert.AreEqual(CheckValues2[ChangeType.Update.ToString()].Count, 1);

            Assert.AreEqual(CheckValues2[ChangeType.Update.ToString()][0].Id, 1);
            Assert.AreEqual(CheckValues2[ChangeType.Update.ToString()][0].Surname, "DEL BIANCO");
            Assert.AreEqual(CheckValues2[ChangeType.Update.ToString()][0].Name, "CHRISTIAN");
            Assert.AreEqual(CheckValues2[ChangeType.Update.ToString()][0].City, "BAAR");
            Assert.AreEqual(CheckValuesOld2[ChangeType.Update.ToString()][0].Id, 1);
            Assert.AreEqual(CheckValuesOld2[ChangeType.Update.ToString()][0].Surname, "DELBIANCO");
            Assert.AreEqual(CheckValuesOld2[ChangeType.Update.ToString()][0].Name, "CHRISTIAN");
            Assert.AreEqual(CheckValuesOld2[ChangeType.Update.ToString()][0].City, "LAVENA PONTE TRESA");

            Assert.AreEqual(CheckValues2[ChangeType.Delete.ToString()].Count, 0);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private void TableDependency_Changed1(object sender, RecordChangedEventArgs<Issue66Model1> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues1[ChangeType.Insert.ToString()].Add(e.Entity);
                    break;

                case ChangeType.Update:
                    CheckValues1[ChangeType.Update.ToString()].Add(e.Entity);
                    CheckValuesOld1[ChangeType.Update.ToString()].Add(e.EntityOldValues);
                    break;

                case ChangeType.Delete:
                    CheckValues1[ChangeType.Delete.ToString()].Add(e.Entity);
                    break;
            }
        }

        private void TableDependency_Changed2(object sender, RecordChangedEventArgs<Issue66Model2> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    CheckValues2[ChangeType.Insert.ToString()].Add(e.Entity);
                    break;

                case ChangeType.Update:
                    CheckValues2[ChangeType.Update.ToString()].Add(e.Entity);
                    CheckValuesOld2[ChangeType.Update.ToString()].Add(e.EntityOldValues);
                    break;

                case ChangeType.Delete:
                    CheckValues2[ChangeType.Delete.ToString()].Add(e.Entity);
                    break;
            }
        }

        private static void ModifyTableContent1()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}1] ([Id], [Name], [City]) VALUES(1, 'CHRISTIAN', 'LAVENA PONTE TRESA')";                    
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}1] ([Id], [Name], [City]) VALUES(2, 'VALENTINA', 'LAVENA PONTE TRESA')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}1] SET [City] = 'BAAR', [Name] = LOWER([Name])";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}1] WHERE [Id] = 1";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}1] WHERE [Id] = 2";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        private static void ModifyTableContent2()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}2] ([Id], [Name], [Second Name], [City]) VALUES(1, 'CHRISTIAN', 'DEL BIANCO', 'LAVENA PONTE TRESA')";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}2] ([Id], [Name], [Second Name], [City]) VALUES(2, 'VALENTINA', 'DEL BIANCO', 'LAVENA PONTE TRESA')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}2] SET [City] = 'BAAR', [Second Name] = LOWER([Second Name])";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"UPDATE [{TableName}2] SET [Name] = LOWER([Name])";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}2] WHERE [Id] = 1";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}2] WHERE [Id] = 2";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        private static void ModifyTableContent5()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}2] ([Id], [Name], [Second Name], [City]) VALUES(1, 'CHRISTIAN', 'DELBIANCO', 'LAVENA PONTE TRESA')";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}2] ([Id], [Name], [Second Name], [City]) VALUES(2, 'LEONARDO', 'DA VINCI', 'ROMA')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{TableName}2] SET [City] = 'BAAR', [Second Name] = 'DEL BIANCO'";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"UPDATE [{TableName}2] SET [Name] = LOWER([Name])";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}2]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}