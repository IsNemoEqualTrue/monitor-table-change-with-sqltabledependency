////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Reflection;
using TableDependency.Classes;
using TableDependency.Enums;
using TableDependency.Mappers;
using TableDependency.Messages;
using TableDependency.Utilities;

namespace TableDependency.EventArgs
{
    public class RecordChangedEventArgs<T> : System.EventArgs where T : class
    {
        protected readonly IEnumerable<PropertyInfo> EntiyProperiesInfo;
        protected IEnumerable<ColumnInfo> UserInterestedColumns;

        #region Properties

        internal MessagesBag MessagesBag { get; }
        public T Entity { get; protected set; }
        public ChangeType ChangeType { get; protected set; }
        public string MessageType { get; protected set; }

        #endregion

        #region Constructors

        internal RecordChangedEventArgs(MessagesBag messagesBag, ModelToTableMapper<T> mapper, IEnumerable<ColumnInfo> userInterestedColumns)
        {
            this.MessagesBag = messagesBag;
            this.EntiyProperiesInfo = ModelUtil.GetModelPropertiesInfo<T>();
            this.UserInterestedColumns = userInterestedColumns;

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
                var columnName = propertyMappedTo ?? entityPropertyInfo.Name;

                var message = messages.FirstOrDefault(m => string.Equals(m.Recipient, columnName, StringComparison.CurrentCultureIgnoreCase));
                if (message == default(Message)) continue;

                var dbColumnInfo = this.GetColumnInfo(columnName);

                var value = GetValue(entityPropertyInfo, dbColumnInfo, message.Body);
                entityPropertyInfo.SetValue(entity, value);
            }

            return entity;
        }

        internal virtual ColumnInfo GetColumnInfo(string columnName)
        {
            return this.UserInterestedColumns.First(uic => string.Equals(uic.Name, columnName, StringComparison.CurrentCultureIgnoreCase));
        }

        internal virtual object GetValue(PropertyInfo entityPropertyInfo, ColumnInfo columnInfo, byte[] message)
        {
            return TypeDescriptor
                .GetConverter(entityPropertyInfo.PropertyType)
                .ConvertFromString(null, culture: CultureInfo.CurrentCulture, text: this.MessagesBag.Encoding.GetString(message).ToString(CultureInfo.CurrentCulture));
        }

        #endregion
    }
}