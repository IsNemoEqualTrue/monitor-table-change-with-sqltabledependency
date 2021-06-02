#region License

// TableDependency, SqlTableDependency
// Copyright (c) 2015-2020 Christian Del Bianco. All rights reserved.
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

#endregion License

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.Exceptions;
using TableDependency.SqlClient.Base.Messages;
using TableDependency.SqlClient.Base.Utilities;

namespace TableDependency.SqlClient.Base.EventArgs
{
    public class DynamicRecordChangedEventArgs : BaseEventArgs
    {
        #region Instance variables

        protected MessagesBag MessagesBag { get; }

        #endregion Instance variables

        #region Properties

        public IDictionary<string, object> Entity { get; protected set; }
        public IDictionary<string, object> EntityOldValues { get; protected set; }
        public ChangeType ChangeType { get; protected set; }

        #endregion Properties

        #region Constructors

        public DynamicRecordChangedEventArgs(
            MessagesBag messagesBag,
            string server,
            string database,
            string sender,
            CultureInfo cultureInfo,
            bool includeOldValues = false) : base(server, database, sender, cultureInfo)
        {
            this.MessagesBag = messagesBag;

            this.ChangeType = messagesBag.MessageType;
            this.Entity = this.MaterializeEntity(messagesBag.Messages.Where(m => !m.IsOldValue).ToList());

            if (includeOldValues && this.ChangeType == ChangeType.Update)
            {
                this.EntityOldValues = this.MaterializeEntity(messagesBag.Messages.Where(m => m.IsOldValue).ToList());
            }
            else
            {
                this.EntityOldValues = new Dictionary<string, object>();
            }
        }

        #endregion Constructors

        #region public methods

        public virtual object GetValue(PropertyInfo propertyInfo, TableColumnInfo columnInfo, byte[] message)
        {
            var stringValue = Convert.ToString(this.MessagesBag.Encoding.GetString(message), base.CultureInfo);
            return this.GetValueObject(propertyInfo, stringValue);
        }

        #endregion public methods

        #region Protected Methods

        protected virtual object GetValueObject(PropertyInfo propertyInfo, string value)
        {
            var propertyType = Nullable.GetUnderlyingType(propertyInfo.PropertyType) ?? propertyInfo.PropertyType;
            var typeCode = Type.GetTypeCode(propertyType);

            try
            {
                switch (typeCode)
                {
                    case TypeCode.Boolean:
                        return bool.Parse(value);

                    case TypeCode.Char:
                        return char.Parse(value);

                    case TypeCode.SByte:
                        return sbyte.Parse(value, base.CultureInfo);

                    case TypeCode.Byte:
                        return byte.Parse(value, base.CultureInfo);

                    case TypeCode.Int16:
                        return short.Parse(value, base.CultureInfo);

                    case TypeCode.UInt16:
                        return ushort.Parse(value, base.CultureInfo);

                    case TypeCode.Int32:
                        return int.Parse(value, base.CultureInfo);

                    case TypeCode.UInt32:
                        return uint.Parse(value, base.CultureInfo);

                    case TypeCode.Int64:
                        return long.Parse(value, base.CultureInfo);

                    case TypeCode.UInt64:
                        return ulong.Parse(value, base.CultureInfo);

                    case TypeCode.Single:
                        return float.Parse(value, base.CultureInfo);

                    case TypeCode.Double:
                        return double.Parse(value, base.CultureInfo);

                    case TypeCode.Decimal:
                        return decimal.Parse(value, base.CultureInfo);

                    case TypeCode.DateTime:
                        return DateTime.Parse(value, base.CultureInfo);

                    case TypeCode.String:
                        return value;

                    case TypeCode.Object:
                        Guid guid;
                        if (Guid.TryParse(value, out guid)) return guid;

                        TimeSpan timeSpan;
                        if (TimeSpan.TryParse(value, out timeSpan)) return timeSpan;

                        DateTimeOffset dateTimeOffset;
                        if (DateTimeOffset.TryParse(value, out dateTimeOffset)) return dateTimeOffset;

                        break;
                }
            }
            catch
            {
                var errorMessage = $"Propery {propertyInfo.Name} cannot be set with db value {value}";
                throw new NoMatchBetweenModelAndTableColumns(errorMessage);
            }

            return null;
        }

        protected virtual TableColumnInfo GetColumnInfo(string columnName)
        {
            return null;
            // return this.UserInterestedColumns.First(uic => string.Equals(uic.Name, columnName, StringComparison.CurrentCultureIgnoreCase));
        }

        protected virtual IDictionary<string, object> MaterializeEntity(List<Message> messages)
        {
            var row = new Dictionary<string, object>();
            foreach (var message in messages)
            {
                var stringValue = Convert.ToString(this.MessagesBag.Encoding.GetString(message.Body), base.CultureInfo);
                row.Add(message.Recipient, stringValue);
            }

            return row;
        }

        protected virtual bool IsNullableType(Type type)
        {
            return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
        }

        #endregion Protected Methods
    }
}