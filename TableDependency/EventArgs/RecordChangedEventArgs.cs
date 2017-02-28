#region License
// TableDependency, SqlTableDependency
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
using System.Linq;
using System.Reflection;
using TableDependency.Classes;
using TableDependency.Enums;
using TableDependency.Mappers;
using TableDependency.Messages;
using TableDependency.Utilities;

namespace TableDependency.EventArgs
{
    public class RecordChangedEventArgs<T> : BaseEventArgs where T : class
    {
        #region Instance variables

        protected readonly IEnumerable<PropertyInfo> EntiyProperiesInfo;
        protected IEnumerable<ColumnInfo> UserInterestedColumns;

        #endregion

        #region Properties

        internal MessagesBag MessagesBag { get; }
        public T Entity { get; protected set; }
        public ChangeType ChangeType { get; protected set; }
        public string MessageType { get; protected set; }

        #endregion

        #region Constructors

        public RecordChangedEventArgs(
            MessagesBag messagesBag,
            ModelToTableMapper<T> mapper,
            IEnumerable<ColumnInfo> userInterestedColumns,
            string server,
            string database,
            string sender) : base(server, database, sender)
        {
            this.MessagesBag = messagesBag;
            this.EntiyProperiesInfo = ModelUtil.GetModelPropertiesInfo<T>();
            this.UserInterestedColumns = userInterestedColumns;

            this.ChangeType = messagesBag.MessageType;
            this.Entity = MaterializeEntity(messagesBag.MessageSheets, mapper);
        }

        #endregion

        #region public methods

        public virtual object GetValue(PropertyInfo propertyInfo, ColumnInfo columnInfo, byte[] message)
        {
            var stringValue = Convert.ToString(this.MessagesBag.Encoding.GetString(message), base.Culture);
            return this.GetValueObject(propertyInfo, stringValue);
        }

        #endregion

        #region Protected Methods

        protected virtual object GetValueObject(PropertyInfo propertyInfo, string value)
        {
            var propertyType = Nullable.GetUnderlyingType(propertyInfo.PropertyType) ?? propertyInfo.PropertyType;
            var typeCode = Type.GetTypeCode(propertyType);

            switch (typeCode)
            {
                case TypeCode.DBNull:
                    return null;

                case TypeCode.Boolean:
                    return Boolean.Parse(value);

                case TypeCode.Char:
                    return Char.Parse(value);

                case TypeCode.SByte:
                    return SByte.Parse(value);

                case TypeCode.Byte:
                    return Byte.Parse(value);

                case TypeCode.Int16:
                    return Int16.Parse(value);

                case TypeCode.UInt16:
                    return UInt16.Parse(value);

                case TypeCode.Int32:
                    return Int32.Parse(value);

                case TypeCode.UInt32:
                    return UInt32.Parse(value);

                case TypeCode.Int64:
                    return Int64.Parse(value);

                case TypeCode.UInt64:
                    return UInt64.Parse(value);

                case TypeCode.Single:
                    return Single.Parse(value);

                case TypeCode.Double:
                    return Double.Parse(value);

                case TypeCode.Decimal:
                    return Decimal.Parse(value);

                case TypeCode.DateTime:
                    return DateTime.Parse(value);
                    //return DateTime.ParseExact("yyyy-MM-dd", value, CultureInfo.InvariantCulture);

                case TypeCode.String:
                    return value as string;

                case TypeCode.Object:
                    Guid guid;
                    if (Guid.TryParse(value, out guid)) return guid;

                    TimeSpan timeSpan;
                    if (TimeSpan.TryParse(value, out timeSpan)) return timeSpan;

                    DateTimeOffset dateTimeOffset;
                    if (DateTimeOffset.TryParse(value, out dateTimeOffset)) return dateTimeOffset;

                    break;
            }

            throw new ArgumentOutOfRangeException();
        }

        protected virtual ColumnInfo GetColumnInfo(string columnName)
        {
            return this.UserInterestedColumns.First(uic => string.Equals(uic.Name, columnName, StringComparison.CurrentCultureIgnoreCase));
        }

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

        #endregion
    }
}