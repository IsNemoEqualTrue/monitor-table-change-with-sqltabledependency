using System; 
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
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
    public class DataAnnotationNotMappedTest2 : Base.SqlTableDependencyBaseTest
    {
        [Table("DataAnnotationNotMappedTest2Model")]
        private class DataAnnotationNotMappedTest2Model
        {
            public long Id { get; set; }
            public string Name { get; set; }
            public string Description { get; set; }

            [NotMapped]
            public string ComposedName { get => this.Id + "-" + this.Name; set => this.Name = value; }
        }
    
        private const string ScemaName = "[dbo]";
        private static int _counter;
        private static Dictionary<string, Tuple<DataAnnotationNotMappedTest2Model, DataAnnotationNotMappedTest2Model>> _checkValuesTest2 = new Dictionary<string, Tuple<DataAnnotationNotMappedTest2Model, DataAnnotationNotMappedTest2Model>>();

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = "IF OBJECT_ID('DataAnnotationNotMappedTest2Model', 'U') IS NOT NULL DROP TABLE [DataAnnotationNotMappedTest2Model];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText = "CREATE TABLE [DataAnnotationNotMappedTest2Model]([Id] [int] NOT NULL, [Name] [NVARCHAR](50) NULL, [Long Description] [NVARCHAR](255) NULL)";
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
                    sqlCommand.CommandText = "DELETE FROM [DataAnnotationNotMappedTest2Model];";
                    sqlCommand.ExecuteNonQuery();
                }
            }

            _checkValuesTest2.Clear();

            _counter = 0;

            _checkValuesTest2.Add(ChangeType.Insert.ToString(), new Tuple<DataAnnotationNotMappedTest2Model, DataAnnotationNotMappedTest2Model>(new DataAnnotationNotMappedTest2Model { Id = 1, Name = "Christian", Description = "Del Bianco" }, new DataAnnotationNotMappedTest2Model()));
            _checkValuesTest2.Add(ChangeType.Update.ToString(), new Tuple<DataAnnotationNotMappedTest2Model, DataAnnotationNotMappedTest2Model>(new DataAnnotationNotMappedTest2Model { Id = 3, Name = "Velia", Description = "Ceccarelli" }, new DataAnnotationNotMappedTest2Model()));
            _checkValuesTest2.Add(ChangeType.Delete.ToString(), new Tuple<DataAnnotationNotMappedTest2Model, DataAnnotationNotMappedTest2Model>(new DataAnnotationNotMappedTest2Model { Id = 3, Name = "Velia", Description = "Ceccarelli" }, new DataAnnotationNotMappedTest2Model()));
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = "IF OBJECT_ID('DataAnnotationNotMappedTest2Model', 'U') IS NOT NULL DROP TABLE [DataAnnotationNotMappedTest2Model];";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestCategory("SqlServer")]
        [TestMethod]
        public void Test2()
        {
            SqlTableDependency<DataAnnotationNotMappedTest2Model> tableDependency = null;
            string naming;

            try
            {
                var mapper = new ModelToTableMapper<DataAnnotationNotMappedTest2Model>();
                mapper.AddMapping(c => c.Description, "Long Description");

                tableDependency = new SqlTableDependency<DataAnnotationNotMappedTest2Model>(
                    ConnectionStringForTestUser, 
                    includeOldValues: false,
                    schemaName: ScemaName, 
                    mapper: mapper);

                tableDependency.OnChanged += TableDependency_Changed_Test2;
                tableDependency.Start();
                naming = tableDependency.DataBaseObjectsNamingConvention;

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContentTest2);
                t.Start();
                Thread.Sleep(1000 * 15 * 1);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.AreEqual(_counter, 3);

            Assert.AreEqual(_checkValuesTest2[ChangeType.Insert.ToString()].Item2.Id, _checkValuesTest2[ChangeType.Insert.ToString()].Item1.Id);
            Assert.AreEqual(_checkValuesTest2[ChangeType.Insert.ToString()].Item2.Name, _checkValuesTest2[ChangeType.Insert.ToString()].Item1.Name);
            Assert.AreEqual(_checkValuesTest2[ChangeType.Insert.ToString()].Item2.Description, _checkValuesTest2[ChangeType.Insert.ToString()].Item1.Description);
            Assert.AreEqual(_checkValuesTest2[ChangeType.Insert.ToString()].Item2.ComposedName, _checkValuesTest2[ChangeType.Insert.ToString()].Item1.ComposedName);

            Assert.AreEqual(_checkValuesTest2[ChangeType.Update.ToString()].Item2.Id, _checkValuesTest2[ChangeType.Update.ToString()].Item1.Id);
            Assert.AreEqual(_checkValuesTest2[ChangeType.Update.ToString()].Item2.Name, _checkValuesTest2[ChangeType.Update.ToString()].Item1.Name);
            Assert.AreEqual(_checkValuesTest2[ChangeType.Update.ToString()].Item2.Description, _checkValuesTest2[ChangeType.Update.ToString()].Item1.Description);
            Assert.AreEqual(_checkValuesTest2[ChangeType.Update.ToString()].Item2.ComposedName, _checkValuesTest2[ChangeType.Update.ToString()].Item1.ComposedName);

            Assert.AreEqual(_checkValuesTest2[ChangeType.Delete.ToString()].Item2.Id, _checkValuesTest2[ChangeType.Delete.ToString()].Item1.Id);
            Assert.AreEqual(_checkValuesTest2[ChangeType.Delete.ToString()].Item2.Name, _checkValuesTest2[ChangeType.Delete.ToString()].Item1.Name);
            Assert.AreEqual(_checkValuesTest2[ChangeType.Delete.ToString()].Item2.Description, _checkValuesTest2[ChangeType.Delete.ToString()].Item1.Description);
            Assert.AreEqual(_checkValuesTest2[ChangeType.Delete.ToString()].Item2.ComposedName, _checkValuesTest2[ChangeType.Delete.ToString()].Item1.ComposedName);

            Assert.IsTrue(base.AreAllDbObjectDisposed(naming));
            Assert.IsTrue(base.CountConversationEndpoints(naming) == 0);
        }

        private static void TableDependency_Changed_Test2(object sender, RecordChangedEventArgs<DataAnnotationNotMappedTest2Model> e)
        {
            _counter++;

            switch (e.ChangeType)
            {
                case ChangeType.Insert:
                    _checkValuesTest2[ChangeType.Insert.ToString()].Item2.Id = e.Entity.Id;
                    _checkValuesTest2[ChangeType.Insert.ToString()].Item2.Name = e.Entity.Name;
                    _checkValuesTest2[ChangeType.Insert.ToString()].Item2.Description = e.Entity.Description;

                    break;

                case ChangeType.Update:
                    _checkValuesTest2[ChangeType.Update.ToString()].Item2.Id = e.Entity.Id;
                    _checkValuesTest2[ChangeType.Update.ToString()].Item2.Name = e.Entity.Name;
                    _checkValuesTest2[ChangeType.Update.ToString()].Item2.Description = e.Entity.Description;

                    break;

                case ChangeType.Delete:
                    _checkValuesTest2[ChangeType.Delete.ToString()].Item2.Id = e.Entity.Id;
                    _checkValuesTest2[ChangeType.Delete.ToString()].Item2.Name = e.Entity.Name;
                    _checkValuesTest2[ChangeType.Delete.ToString()].Item2.Description = e.Entity.Description;

                    break;
            }
        }    

        private static void ModifyTableContentTest2()
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForTestUser))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [DataAnnotationNotMappedTest2Model] ([Id], [Name], [Long Description]) VALUES ({_checkValuesTest2[ChangeType.Insert.ToString()].Item1.Id}, '{_checkValuesTest2[ChangeType.Insert.ToString()].Item1.Name}', '{_checkValuesTest2[ChangeType.Insert.ToString()].Item1.Description}')";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"UPDATE [DataAnnotationNotMappedTest2Model] SET [Id] = {_checkValuesTest2[ChangeType.Update.ToString()].Item1.Id}, [Name] = '{_checkValuesTest2[ChangeType.Update.ToString()].Item1.Name}', [Long Description] = '{_checkValuesTest2[ChangeType.Update.ToString()].Item1.Description}'";
                    sqlCommand.ExecuteNonQuery();
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = "DELETE FROM [DataAnnotationNotMappedTest2Model]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }
    }
}