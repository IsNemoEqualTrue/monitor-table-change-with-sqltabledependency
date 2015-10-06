////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.Messages;
using TableDependency.SqlClient.Extensions;
using TableDependency.SqlClient.Messages;
using TableDependency.Utilities;

namespace TableDependency.SqlClient.EventArgs
{
    public sealed class SqlRecordChangedEventArgs<T> : RecordChangedEventArgs<T> where T : class
    {
        #region Member variables

        private static readonly IEnumerable<PropertyInfo> _entiyProperiesInfo;

        #endregion

        #region Properties

        /// <summary>
        /// Gets modified record.
        /// </summary>
        /// <value>
        /// The changed entity.
        /// </value>
        public override T Entity { get; protected set; }

        /// <summary>
        /// Gets the change type (insert/delete/update).
        /// </summary>
        /// <value>
        /// The change action.
        /// </value>
        public override ChangeType ChangeType { get; protected set; }

        /// <summary>
        /// Gets SQL message type.
        /// </summary>
        /// <value>
        /// The type of the message.
        /// </value>
        public override string MessageType { get; protected set; }

        #endregion

        #region Constructors

        static SqlRecordChangedEventArgs()
        {
            _entiyProperiesInfo = ModelUtil.GetModelPropertiesInfo<T>();
        }

        internal SqlRecordChangedEventArgs(MessagesBag messagesBag, ModelToTableMapper<T> mapper)
        {
            ChangeType = messagesBag.MessageType;
            Entity = MaterializeEntity(messagesBag.MessageSheets, mapper);
        }

        #endregion

        #region Private methods

        private static T MaterializeEntity(List<Message> messages, ModelToTableMapper<T> mapper)
        {
            var entity = (T)Activator.CreateInstance(typeof(T));

            foreach (var entityPropertyInfo in _entiyProperiesInfo)
            {
                var propertyMappedTo = mapper?.GetMapping(entityPropertyInfo);
                var propertyName = propertyMappedTo ?? entityPropertyInfo.Name;

                var message = messages.FirstOrDefault(m => string.Equals(m.Recipient, propertyName, StringComparison.CurrentCultureIgnoreCase));
                if (message == default(Message)) continue;

                var value = GetValue(entityPropertyInfo, message.Body);
                entityPropertyInfo.SetValue(entity, value);
            }

            return entity;
        }

        private static object GetValue(PropertyInfo entityPropertyInfo, byte[] message)
        {
            if (message == null || message.Length == 0) return null;

            if (entityPropertyInfo.PropertyType == typeof(byte[])) return message;

            if (entityPropertyInfo.PropertyType == typeof(bool) || entityPropertyInfo.PropertyType == typeof(bool?)) return Encoding.Unicode.GetString(message).ToBoolean();

            if (entityPropertyInfo.PropertyType == typeof(char[])) return Encoding.Unicode.GetString(message).ToCharArray();
            
            return TypeDescriptor.GetConverter(entityPropertyInfo.PropertyType).ConvertFromString(Encoding.Unicode.GetString(message));
        }

        #endregion
    }
}