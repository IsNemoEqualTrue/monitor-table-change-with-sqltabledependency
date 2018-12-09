#region License
// TableDependency, SqlTableDependency - Release 8.5.2
// Copyright (c) 2015-2019 Christian Del Bianco. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation
// files (the "Software"), to deal in the Software without
// restriction, including without limitation the rights to use,
// copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
#endregion

using System;
using System.Configuration;
using System.Data.SqlClient;

namespace TableDependency.SqlClient.Test.Base
{
    public abstract class SqlTableDependencyBaseTest
    {
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
            Buffer.BlockCopy(bytes, 0, chars, 0, bytes.Length);
            return new string(chars);
        }
    }
}