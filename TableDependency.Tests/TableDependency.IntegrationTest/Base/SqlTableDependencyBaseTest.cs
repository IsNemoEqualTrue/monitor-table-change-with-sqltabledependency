using System;
using System.Configuration;
using System.Data.SqlClient;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TableDependency.IntegrationTest.Base
{
    public abstract class SqlTableDependencyBaseTest
    {
        public TestContext TestContext { get; set; }
        protected static readonly string ConnectionStringForTestUser = ConfigurationManager.ConnectionStrings["SqlServer2008 Test_User"].ConnectionString;
        protected static readonly string ConnectionStringForSa = ConfigurationManager.ConnectionStrings["SqlServer2008 sa"].ConnectionString;

        protected bool AreAllDbObjectDisposed(string naming)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForSa))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"SELECT COUNT(*) FROM sys.objects WITH (NOLOCK) WHERE name = N'tr_{naming}_Sender'";
                    var triggerExistis = Convert.ToInt32(sqlCommand.ExecuteScalar());

                    sqlCommand.CommandText = $"SELECT COUNT(*) FROM sys.service_message_types WITH (NOLOCK) WHERE name = N'{naming}_Updated'";
                    var messageExistis = Convert.ToInt32(sqlCommand.ExecuteScalar());

                    sqlCommand.CommandText = $"SELECT COUNT(*) FROM sys.objects WITH (NOLOCK) WHERE name = N'{naming}_QueueActivationSender'";
                    var procedureExistis1 = Convert.ToInt32(sqlCommand.ExecuteScalar());

                    return triggerExistis == 0 && messageExistis == 0 && procedureExistis1 == 0;
                }
            }
        }

        protected int CountConversationEndpoints(string naming = null)
        {
            using (var sqlConnection = new SqlConnection(ConnectionStringForSa))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = "select COUNT(*) from sys.conversation_endpoints WITH (NOLOCK)" + (string.IsNullOrWhiteSpace(naming) ? ";" : $" WHERE [far_service] = '{naming}_Receiver';");
                    return (int)sqlCommand.ExecuteScalar();
                }
            }
        }

        protected byte[] GetBytes(string str, int? lenght = null)
        {
            if (str == null) return null;

            byte[] bytes = lenght.HasValue ? new byte[lenght.Value] : new byte[str.Length * sizeof(char)];
            Buffer.BlockCopy(str.ToCharArray(), 0, bytes, 0, str.Length * sizeof(char));
            return bytes;
        }

        protected string GetString(byte[] bytes)
        {
            if (bytes == null) return null;

            char[] chars = new char[bytes.Length / sizeof(char)];
            System.Buffer.BlockCopy(bytes, 0, chars, 0, bytes.Length);
            return new string(chars);
        }
    }
}