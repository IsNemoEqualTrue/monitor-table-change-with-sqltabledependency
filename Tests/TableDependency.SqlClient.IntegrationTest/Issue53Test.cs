using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.SqlClient.BaseTests;

namespace TableDependency.SqlClient.IntegrationTests
{
    public enum SexEnum
    {
        Male,
        Female
    }

    internal class Issue53Model1
    {
        public int Id { get; set; }
        public SexEnum? Gender { get; set; }
    }

    internal class Issue53Model2
    {
        public int Id { get; set; }
        public SexEnum Gender { get; set; }
    }

    internal class Issue53Model3
    {
        public int Id { get; set; }
        public SexEnum? Gender { get; set; }
    }

    internal class Issue53Model4
    {
        public int Id { get; set; }
        public SexEnum Gender { get; set; }
    }

    [TestClass]
    public class Issue53Test : SqlTableDependencyBaseTest
    {        
        private static readonly string TableName1 = typeof(Issue53Model1).Name;
        private static readonly string TableName2 = typeof(Issue53Model2).Name;
        private static readonly string TableName3 = typeof(Issue53Model3).Name;
        private static readonly string TableName4 = typeof(Issue53Model4).Name;
        private static Dictionary<string, Tuple<Issue53Model1, Issue53Model1>> _checkValues1 = new Dictionary<string, Tuple<Issue53Model1, Issue53Model1>>();
        private static Dictionary<string, Tuple<Issue53Model2, Issue53Model2>> _checkValues2 = new Dictionary<string, Tuple<Issue53Model2, Issue53Model2>>();
        private static Dictionary<string, Tuple<Issue53Model3, Issue53Model3>> _checkValues3 = new Dictionary<string, Tuple<Issue53Model3, Issue53Model3>>();
        private static Dictionary<string, Tuple<Issue53Model4, Issue53Model4>> _checkValues4 = new Dictionary<string, Tuple<Issue53Model4, Issue53Model4>>();

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('[{TableName1}]', 'U') IS NOT NULL DROP TABLE [dbo].[{TableName1}]";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"CREATE TABLE [{TableName1}]([Id] [int] NULL, [Gender] [int])";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"IF OBJECT_ID('[{TableName2}]', 'U') IS NOT NULL DROP TABLE [dbo].[{TableName2}]";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"CREATE TABLE [{TableName2}]([Id] [int] NULL, [Gender] [int])";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"IF OBJECT_ID('[{TableName3}]', 'U') IS NOT NULL DROP TABLE [dbo].[{TableName3}]";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"CREATE TABLE [{TableName3}]([Id] [int] NULL, [Gender] [int] NULL)";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"IF OBJECT_ID('[{TableName4}]', 'U') IS NOT NULL DROP TABLE [dbo].[{TableName4}]";
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = $"CREATE TABLE [{TableName4}]([Id] [int] NULL, [Gender] [int] NULL)";
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
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName1}', 'U') IS NOT NULL DROP TABLE [{TableName1}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName2}', 'U') IS NOT NULL DROP TABLE [{TableName2}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName3}', 'U') IS NOT NULL DROP TABLE [{TableName3}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName4}', 'U') IS NOT NULL DROP TABLE [{TableName4}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void Test1()
        {
            string objectNaming;
            var tableDependency = new SqlTableDependency<Issue53Model1>(ConnectionStringForTestUser, includeOldValues: true, tableName: TableName1);

            try
            {              
                tableDependency.OnChanged += TableDependency_Changed1;
                tableDependency.Start();
                objectNaming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent1);
                t.Start();
                Thread.Sleep(1000 * 10 * 1);
            }
            finally
            {
                tableDependency.Dispose();
            }

            Assert.AreEqual(_checkValues1[ChangeType.Insert.ToString()].Item2.Id, _checkValues1[ChangeType.Insert.ToString()].Item1.Id);
            Assert.AreEqual(_checkValues1[ChangeType.Insert.ToString()].Item2.Gender, _checkValues1[ChangeType.Insert.ToString()].Item1.Gender);
            Assert.AreEqual(_checkValues1[ChangeType.Update.ToString()].Item2.Id, _checkValues1[ChangeType.Update.ToString()].Item1.Id);
            Assert.AreEqual(_checkValues1[ChangeType.Update.ToString()].Item2.Gender, _checkValues1[ChangeType.Update.ToString()].Item1.Gender);
            Assert.AreEqual(_checkValues1[ChangeType.Delete.ToString()].Item2.Id, _checkValues1[ChangeType.Delete.ToString()].Item1.Id);
            Assert.AreEqual(_checkValues1[ChangeType.Delete.ToString()].Item2.Gender, _checkValues1[ChangeType.Delete.ToString()].Item1.Gender);

            Assert.IsTrue(base.AreAllDbObjectDisposed(objectNaming));
            Assert.IsTrue(base.CountConversationEndpoints(objectNaming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void Test2()
        {
            string objectNaming;
            var tableDependency = new SqlTableDependency<Issue53Model2>(ConnectionStringForTestUser, includeOldValues: true, tableName: TableName2);

            try
            {
                tableDependency.OnChanged += TableDependency_Changed2;
                tableDependency.Start();
                objectNaming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent2);
                t.Start();
                Thread.Sleep(1000 * 10 * 1);
            }
            finally
            {
                tableDependency.Dispose();
            }

            Assert.AreEqual(_checkValues2[ChangeType.Insert.ToString()].Item2.Id, _checkValues2[ChangeType.Insert.ToString()].Item1.Id);
            Assert.AreEqual(_checkValues2[ChangeType.Insert.ToString()].Item2.Gender, _checkValues2[ChangeType.Insert.ToString()].Item1.Gender);
            Assert.AreEqual(_checkValues2[ChangeType.Update.ToString()].Item2.Id, _checkValues2[ChangeType.Update.ToString()].Item1.Id);
            Assert.AreEqual(_checkValues2[ChangeType.Update.ToString()].Item2.Gender, _checkValues2[ChangeType.Update.ToString()].Item1.Gender);
            Assert.AreEqual(_checkValues2[ChangeType.Delete.ToString()].Item2.Id, _checkValues2[ChangeType.Delete.ToString()].Item1.Id);
            Assert.AreEqual(_checkValues2[ChangeType.Delete.ToString()].Item2.Gender, _checkValues2[ChangeType.Delete.ToString()].Item1.Gender);

            Assert.IsTrue(base.AreAllDbObjectDisposed(objectNaming));
            Assert.IsTrue(base.CountConversationEndpoints(objectNaming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void Test3()
        {
            string objectNaming;
            var tableDependency = new SqlTableDependency<Issue53Model3>(ConnectionStringForTestUser, tableName: TableName3);

            try
            {
                tableDependency.OnChanged += TableDependency_Changed3;
                tableDependency.Start();
                objectNaming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent3);
                t.Start();
                Thread.Sleep(1000 * 10 * 1);
            }
            finally
            {
                tableDependency.Dispose();
            }

            Assert.AreEqual(_checkValues3[ChangeType.Insert.ToString()].Item2.Id, _checkValues3[ChangeType.Insert.ToString()].Item1.Id);
            Assert.AreEqual(_checkValues3[ChangeType.Insert.ToString()].Item2.Gender, _checkValues3[ChangeType.Insert.ToString()].Item1.Gender);
            Assert.AreEqual(_checkValues3[ChangeType.Update.ToString()].Item2.Id, _checkValues3[ChangeType.Update.ToString()].Item1.Id);
            Assert.AreEqual(_checkValues3[ChangeType.Update.ToString()].Item2.Gender, _checkValues3[ChangeType.Update.ToString()].Item1.Gender);
            Assert.AreEqual(_checkValues3[ChangeType.Delete.ToString()].Item2.Id, _checkValues3[ChangeType.Delete.ToString()].Item1.Id);
            Assert.AreEqual(_checkValues3[ChangeType.Delete.ToString()].Item2.Gender, _checkValues3[ChangeType.Delete.ToString()].Item1.Gender);

            Assert.IsTrue(base.AreAllDbObjectDisposed(objectNaming));
            Assert.IsTrue(base.CountConversationEndpoints(objectNaming) == 0);
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void Test4()
        {
            string objectNaming;
            var tableDependency = new SqlTableDependency<Issue53Model4>(ConnectionStringForTestUser, tableName: TableName4);

            try
            {
                tableDependency.OnChanged += TableDependency_Changed4;
                tableDependency.Start();
                objectNaming = tableDependency.DataBaseObjectsNamingConvention;

                var t = new Task(ModifyTableContent4);
                t.Start();
                Thread.Sleep(1000 * 10 * 1);
            }
            finally
            {
                tableDependency.Dispose();
            }

            Assert.AreEqual(_checkValues4[ChangeType.Insert.ToString()].Item2.Id, _checkValues4[ChangeType.Insert.ToString()].Item1.Id);
            Assert.AreEqual(_checkValues4[ChangeType.Insert.ToString()].Item2.Gender, _checkValues4[ChangeType.Insert.ToString()].Item1.Gender);
            Assert.AreEqual(_checkValues4[ChangeType.Update.ToString()].Item2.Id, _checkValues4[ChangeType.Update.ToString()].Item1.Id);
            Assert.AreEqual(_checkValues4[ChangeType.Update.ToString()].Item2.Gender, _checkValues4[ChangeType.Update.ToString()].Item1.Gender);
            Assert.AreEqual(_checkValues4[ChangeType.Delete.ToString()].Item2.Id, _checkValues4[ChangeType.Delete.ToString()].Item1.Id);
            Assert.AreEqual(_checkValues4[ChangeType.Delete.ToString()].Item2.Gender, _checkValues4[ChangeType.Delete.ToString()].Item1.Gender);

            Assert.IsTrue(base.AreAllDbObjectDisposed(objectNaming));
            Assert.IsTrue(base.CountConversationEndpoints(objectNaming) == 0);
        }

        private static void TableDependency_Changed1(object sender, RecordChangedEventArgs<Issue53Model1> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues1[ChangeType.Insert.ToString()].Item2.Id = e.Entity.Id;
                    _checkValues1[ChangeType.Insert.ToString()].Item2.Gender = e.Entity.Gender;
                    break;

                case ChangeType.Delete:
                    _checkValues1[ChangeType.Delete.ToString()].Item2.Id = e.Entity.Id;
                    _checkValues1[ChangeType.Delete.ToString()].Item2.Gender = e.Entity.Gender;
                    break;

                case ChangeType.Update:
                    _checkValues1[ChangeType.Update.ToString()].Item2.Id = e.Entity.Id;
                    _checkValues1[ChangeType.Update.ToString()].Item2.Gender = e.Entity.Gender;
                    break;
            }
        }

        private static void TableDependency_Changed2(object sender, RecordChangedEventArgs<Issue53Model2> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues2[ChangeType.Insert.ToString()].Item2.Id = e.Entity.Id;
                    _checkValues2[ChangeType.Insert.ToString()].Item2.Gender = e.Entity.Gender;
                    break;

                case ChangeType.Delete:
                    _checkValues2[ChangeType.Delete.ToString()].Item2.Id = e.Entity.Id;
                    _checkValues2[ChangeType.Delete.ToString()].Item2.Gender = e.Entity.Gender;
                    break;

                case ChangeType.Update:
                    _checkValues2[ChangeType.Update.ToString()].Item2.Id = e.Entity.Id;
                    _checkValues2[ChangeType.Update.ToString()].Item2.Gender = e.Entity.Gender;
                    break;
            }
        }

        private static void TableDependency_Changed3(object sender, RecordChangedEventArgs<Issue53Model3> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues3[ChangeType.Insert.ToString()].Item2.Id = e.Entity.Id;
                    _checkValues3[ChangeType.Insert.ToString()].Item2.Gender = e.Entity.Gender;
                    break;

                case ChangeType.Delete:
                    _checkValues3[ChangeType.Delete.ToString()].Item2.Id = e.Entity.Id;
                    _checkValues3[ChangeType.Delete.ToString()].Item2.Gender = e.Entity.Gender;
                    break;

                case ChangeType.Update:
                    _checkValues3[ChangeType.Update.ToString()].Item2.Id = e.Entity.Id;
                    _checkValues3[ChangeType.Update.ToString()].Item2.Gender = e.Entity.Gender;
                    break;
            }
        }

        private static void TableDependency_Changed4(object sender, RecordChangedEventArgs<Issue53Model4> e)
        {
            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValues4[ChangeType.Insert.ToString()].Item2.Id = e.Entity.Id;
                    _checkValues4[ChangeType.Insert.ToString()].Item2.Gender = e.Entity.Gender;
                    break;

                case ChangeType.Delete:
                    _checkValues4[ChangeType.Delete.ToString()].Item2.Id = e.Entity.Id;
                    _checkValues4[ChangeType.Delete.ToString()].Item2.Gender = e.Entity.Gender;
                    break;

                case ChangeType.Update:
                    _checkValues4[ChangeType.Update.ToString()].Item2.Id = e.Entity.Id;
                    _checkValues4[ChangeType.Update.ToString()].Item2.Gender = e.Entity.Gender;
                    break;
            }
        }

        private static void ModifyTableContent1()
        {
            _checkValues1.Add(ChangeType.Insert.ToString(), new Tuple<Issue53Model1, Issue53Model1>(new Issue53Model1 { Id = 23, Gender = SexEnum.Female }, new Issue53Model1()));
            _checkValues1.Add(ChangeType.Update.ToString(), new Tuple<Issue53Model1, Issue53Model1>(new Issue53Model1 { Id = 4, Gender = SexEnum.Male }, new Issue53Model1()));
            _checkValues1.Add(ChangeType.Delete.ToString(), new Tuple<Issue53Model1, Issue53Model1>(new Issue53Model1 { Id = 4, Gender = SexEnum.Male }, new Issue53Model1()));

            ModifyTableContent(
                TableName1, 
                _checkValues1[ChangeType.Insert.ToString()].Item1.Id,
                _checkValues1[ChangeType.Insert.ToString()].Item1.Gender.GetHashCode(),
                _checkValues1[ChangeType.Update.ToString()].Item1.Id,
                _checkValues1[ChangeType.Update.ToString()].Item1.Gender.GetHashCode());
        }

        private static void ModifyTableContent2()
        {
            _checkValues2.Add(ChangeType.Insert.ToString(), new Tuple<Issue53Model2, Issue53Model2>(new Issue53Model2 { Id = 9, Gender = SexEnum.Female }, new Issue53Model2()));
            _checkValues2.Add(ChangeType.Update.ToString(), new Tuple<Issue53Model2, Issue53Model2>(new Issue53Model2 { Id = 4, Gender = SexEnum.Male }, new Issue53Model2()));
            _checkValues2.Add(ChangeType.Delete.ToString(), new Tuple<Issue53Model2, Issue53Model2>(new Issue53Model2 { Id = 4, Gender = SexEnum.Male }, new Issue53Model2()));

            ModifyTableContent(
                TableName2,
                _checkValues2[ChangeType.Insert.ToString()].Item1.Id,
                _checkValues2[ChangeType.Insert.ToString()].Item1.Gender.GetHashCode(),
                _checkValues2[ChangeType.Update.ToString()].Item1.Id,
                _checkValues2[ChangeType.Update.ToString()].Item1.Gender.GetHashCode());
        }

        private static void ModifyTableContent3()
        {
            _checkValues3.Add(ChangeType.Insert.ToString(), new Tuple<Issue53Model3, Issue53Model3>(new Issue53Model3 { Id = 7, Gender = SexEnum.Female }, new Issue53Model3()));
            _checkValues3.Add(ChangeType.Update.ToString(), new Tuple<Issue53Model3, Issue53Model3>(new Issue53Model3 { Id = 4, Gender = SexEnum.Male }, new Issue53Model3()));
            _checkValues3.Add(ChangeType.Delete.ToString(), new Tuple<Issue53Model3, Issue53Model3>(new Issue53Model3 { Id = 4, Gender = SexEnum.Male }, new Issue53Model3()));

            ModifyTableContent(
                TableName3,
                _checkValues3[ChangeType.Insert.ToString()].Item1.Id,
                _checkValues3[ChangeType.Insert.ToString()].Item1.Gender.GetHashCode(),
                _checkValues3[ChangeType.Update.ToString()].Item1.Id,
                _checkValues3[ChangeType.Update.ToString()].Item1.Gender.GetHashCode());
        }

        private static void ModifyTableContent4()
        {
            _checkValues4.Add(ChangeType.Insert.ToString(), new Tuple<Issue53Model4, Issue53Model4>(new Issue53Model4 { Id = 57, Gender = SexEnum.Female }, new Issue53Model4()));
            _checkValues4.Add(ChangeType.Update.ToString(), new Tuple<Issue53Model4, Issue53Model4>(new Issue53Model4 { Id = 4, Gender = SexEnum.Male }, new Issue53Model4()));
            _checkValues4.Add(ChangeType.Delete.ToString(), new Tuple<Issue53Model4, Issue53Model4>(new Issue53Model4 { Id = 4, Gender = SexEnum.Male }, new Issue53Model4()));

            ModifyTableContent(
                TableName4,
                _checkValues4[ChangeType.Insert.ToString()].Item1.Id,
                _checkValues4[ChangeType.Insert.ToString()].Item1.Gender.GetHashCode(),
                _checkValues4[ChangeType.Update.ToString()].Item1.Id,
                _checkValues4[ChangeType.Update.ToString()].Item1.Gender.GetHashCode());
        }

        private static void ModifyTableContent(string tableName, int idInsert, int genderInsert, int idUpdate, int genderUpdate)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{tableName}] ([Id], [Gender]) VALUES ({idInsert}, {genderInsert})";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [{tableName}] SET [Id] = {idUpdate}, [Gender] = {genderUpdate}";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{tableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}
