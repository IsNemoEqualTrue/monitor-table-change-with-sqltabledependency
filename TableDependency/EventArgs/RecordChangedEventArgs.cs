#region License
// TableDependency, SqlTableDependency, OracleTableDependency
// Copyright (c) 2015-2017 Christian Del Bianco. All rights reserved.
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

        protected RecordChangedEventArgs(MessagesBag messagesBag, ModelToTableMapper<T> mapper, IEnumerable<ColumnInfo> userInterestedColumns)
        {
            this.MessagesBag = messagesBag;
            this.EntiyProperiesInfo = ModelUtil.GetModelPropertiesInfo<T>();
            this.UserInterestedColumns = userInterestedColumns;

            ChangeType = messagesBag.MessageType;
            Entity = MaterializeEntity(messagesBag.MessageSheets, mapper);
        }

        #endregion

        #region Internal methods

        protected virtual T MaterializeEntity(List<Message> messages, ModelToTableMapper<T> mapper)
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

        public virtual ColumnInfo GetColumnInfo(string columnName)
        {
            return this.UserInterestedColumns.First(uic => string.Equals(uic.Name, columnName, StringComparison.CurrentCultureIgnoreCase));
        }

        public virtual object GetValue(PropertyInfo entityPropertyInfo, ColumnInfo columnInfo, byte[] message)
        {
            var formatCulture = new CultureInfo("en-US", false);
            var stringValue = this.MessagesBag.Encoding.GetString(message).ToString(formatCulture);
            var typeDescriptor = TypeDescriptor.GetConverter(entityPropertyInfo.PropertyType);
            var result = typeDescriptor.ConvertFromString(null, formatCulture, stringValue);
            return result;
        }

        #endregion
    }
}