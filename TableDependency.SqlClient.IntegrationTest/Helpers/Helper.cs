using System;
using System.Data.SqlClient;

namespace TableDependency.SqlClient.IntegrationTest.Helpers
{
    internal static class Helper
    {
        internal static bool AreAllDbObjectDisposed(string connectionString, string naming)
        {
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                using (var sqlCommand = sqlConnection.CreateCommand())
                {
                    sqlCommand.CommandText = $"SELECT COUNT(*) FROM sys.objects WHERE name = N'tr_{naming}'";
                    var triggerExistis = Convert.ToInt32(sqlCommand.ExecuteScalar());

                    sqlCommand.CommandText = $"SELECT COUNT(*) FROM sys.service_message_types WHERE name = N'{naming}_Updated'";
                    var messageExistis = Convert.ToInt32(sqlCommand.ExecuteScalar());

                    sqlCommand.CommandText = $"SELECT COUNT(*) FROM sys.objects WHERE name = N'{naming}_QueueActivation'";
                    var procedureExistis = Convert.ToInt32(sqlCommand.ExecuteScalar());

                    return (triggerExistis == 0 && messageExistis == 0 && procedureExistis == 0);
                }
            }
        }
    }
}