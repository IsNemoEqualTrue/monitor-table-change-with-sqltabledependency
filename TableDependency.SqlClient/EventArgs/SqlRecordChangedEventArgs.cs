////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   © 2015-2106 Christian Del Bianco. All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using TableDependency.Classes;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.Messages;
using TableDependency.SqlClient.Extensions;

namespace TableDependency.SqlClient.EventArgs
{
    public sealed class SqlRecordChangedEventArgs<T> : RecordChangedEventArgs<T> where T : class
    {
        internal SqlRecordChangedEventArgs(MessagesBag messagesBag, ModelToTableMapper<T> mapper, IEnumerable<ColumnInfo> userInterestedColumns) : base(messagesBag, mapper, userInterestedColumns)
        {
        }

        internal override object GetValue(PropertyInfo entityPropertyInfo, ColumnInfo columnInfo, byte[] message)
        {
            if (message == null || message.Length == 0) return null;

            if (entityPropertyInfo.PropertyType == typeof(byte[])) return message;

            if (entityPropertyInfo.PropertyType == typeof(bool) || entityPropertyInfo.PropertyType == typeof(bool?)) return Encoding.Unicode.GetString(message).ToBoolean();

            if (entityPropertyInfo.PropertyType == typeof(char[])) return Encoding.Unicode.GetString(message).ToCharArray();

            return base.GetValue(entityPropertyInfo, columnInfo, message);
        }
    }
}