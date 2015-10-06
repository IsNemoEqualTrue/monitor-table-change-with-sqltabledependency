using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Exceptions;
using TableDependency.Mappers;
using TableDependency.SqlClient.Exceptions;
using TableDependency.SqlClient.IntegrationTest.Model;

namespace TableDependency.SqlClient.IntegrationTest.Issues
{
    [TestClass]
    public class Issue_0003
    {
        private static string _connectionString = ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        private const string TableName = "Issue0003";
        private int _counter;

        [ClassInitialize()]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();

                    sqlCommand.CommandText =
                        $"CREATE TABLE [{TableName}](" +
                        "[Id][int] IDENTITY(1, 1) NOT NULL," +
                        "[FirstName] [varchar](4000) NOT NULL," +
                        "[SecondName] [nvarchar](4000) NOT NULL," +
                        "[NotManagedColumnBecauseIsImage] [image])";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestInitialize()]
        public void TestInitialize()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [ClassCleanup()]
        public static void ClassCleanup()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ColumnTypeNotSupportedException))]
        public void DealWithNotManagedColumnsTypeTest()
        {
            SqlTableDependency<Issue_0003_Model_Unmanaged> tableDependency = null;

            try
            {
                tableDependency = new SqlTableDependency<Issue_0003_Model_Unmanaged>(_connectionString, TableName);
                tableDependency.OnChanged += this.TableDependency_Changed_Unmanaged;
                tableDependency.Start();

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.IsTrue(this._counter == 3);
        }

        [TestMethod]
        public void DealWithManagedColumnsTypeTest()
        {
            SqlTableDependency<Issue_0003_Model_Managed> tableDependency = null;
            var interestedColumnsList = new List<string>() { "FirstName", "SecondName" };

            try
            {
                tableDependency = new SqlTableDependency<Issue_0003_Model_Managed>(_connectionString, TableName, updateOf: interestedColumnsList);
                tableDependency.OnChanged += this.TableDependency_Changed_Managed;
                tableDependency.Start();

                Thread.Sleep(5000);

                var t = new Task(ModifyTableContent);
                t.Start();
                t.Wait(20000);
            }
            finally
            {
                tableDependency?.Dispose();
            }

            Assert.IsTrue(this._counter == 3);
        }

        private void TableDependency_Changed_Managed(object sender, RecordChangedEventArgs<Issue_0003_Model_Managed> e)
        {
            this._counter++;
        }

        private void TableDependency_Changed_Unmanaged(object sender, RecordChangedEventArgs<Issue_0003_Model_Unmanaged> e)
        {
            this._counter++;
        }

        static byte[] GetBytes(string str, int? lenght = null)
        {
            if (str == null) return null;

            byte[] bytes = lenght.HasValue ? new byte[lenght.Value] : new byte[str.Length * sizeof(char)];
            Buffer.BlockCopy(str.ToCharArray(), 0, bytes, 0, str.Length * sizeof(char));
            return bytes;
        }

        private static void ModifyTableContent()
        {
            using (var sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([FirstName],[SecondName],[NotManagedColumnBecauseIsImage]) VALUES ('Valentina', 'Del Bianco', @image)";
                    sqlCommand.Parameters.Add(new SqlParameter("@image", SqlDbType.VarBinary) {Value = GetBytes("Nonna Velia")});

                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [FirstName] = 'ntina'";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {

                    sqlCommand.CommandText = $"UPDATE [{TableName}] SET [NotManagedColumnBecauseIsImage] = 'Valentina Del Bianco'";
                    sqlCommand.Parameters.Add(new SqlParameter("@image", SqlDbType.VarBinary) {Value = GetBytes("Nonna Dirce")});
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);
                }

                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
                    sqlCommand.ExecuteNonQuery();
                    Thread.Sleep(1000);

                }
            }
        }
    }
}