////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System.Reflection;
using System.Text;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.Messages;

namespace TableDependency.OracleClient.EventArgs
{
    public sealed class OracleRecordChangedEventArgs<T> : RecordChangedEventArgs<T> where T : class
    {
        #region Constructors

        internal OracleRecordChangedEventArgs(MessagesBag messagesBag, ModelToTableMapper<T> mapper) : base(messagesBag, mapper)
        {
        }

        #endregion

        #region Internal methods

        internal override object GetValue(PropertyInfo entityPropertyInfo, byte[] message)
        {
            if (message == null || message.Length == 0) return null;

            if (entityPropertyInfo.PropertyType == typeof(byte[])) return message;

            if (entityPropertyInfo.PropertyType == typeof(char[])) return Encoding.Unicode.GetString(message).ToCharArray();

            return base.GetValue(entityPropertyInfo, message);
        }

        #endregion
    }
}