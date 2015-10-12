using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using TableDependency.Enums;
using TableDependency.Mappers;
using TableDependency.Messages;
using TableDependency.Utilities;

namespace TableDependency.EventArgs
{
    ////////////////////////////////////////////////////////////////////////////////
    //   TableDependency, SqlTableDependency, OracleTableDependency
    //   Copyright (c) Christian Del Bianco.  All rights reserved.
    ////////////////////////////////////////////////////////////////////////////////
    public class RecordChangedEventArgs<T> : System.EventArgs where T : class
    {
        protected readonly IEnumerable<PropertyInfo> EntiyProperiesInfo;

        #region Properties

        public T Entity { get; protected set; }
        public ChangeType ChangeType { get; protected set; }
        public string MessageType { get; protected set; }

        #endregion

        #region Constructors

        internal RecordChangedEventArgs(MessagesBag messagesBag, ModelToTableMapper<T> mapper)
        {
            this.EntiyProperiesInfo = ModelUtil.GetModelPropertiesInfo<T>();

            ChangeType = messagesBag.MessageType;
            Entity = MaterializeEntity(messagesBag.MessageSheets, mapper);
        }

        #endregion

        #region Internal methods

        internal virtual T MaterializeEntity(List<Message> messages, ModelToTableMapper<T> mapper)
        {
            var entity = (T)Activator.CreateInstance(typeof(T));

            foreach (var entityPropertyInfo in this.EntiyProperiesInfo)
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

        internal virtual object GetValue(PropertyInfo entityPropertyInfo, byte[] message)
        {
            return TypeDescriptor
                .GetConverter(entityPropertyInfo.PropertyType)
                .ConvertFromString(null, CultureInfo.CurrentCulture, Encoding.Unicode.GetString(message).ToString(CultureInfo.CurrentCulture));
        }

        #endregion
    }
}