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
using System.Reflection;
using System.Text;
using TableDependency.Abstracts;
using TableDependency.EventArgs;
using TableDependency.Messages;
using TableDependency.SqlClient.Extensions;
using TableDependency.Utilities;

namespace TableDependency.SqlClient.EventArgs
{
    public sealed class SqlRecordChangedEventArgs<T> : RecordChangedEventArgs<T> where T : class
    {
        public SqlRecordChangedEventArgs(
            MessagesBag messagesBag,
            IModelToTableMapper<T> mapper,
            IEnumerable<ColumnInfo> userInterestedColumns,
            string server,
            string database,
            string sender) : base(messagesBag, mapper, userInterestedColumns, server, database, sender)
        {

        }

        public override object GetValue(PropertyInfo entityPropertyInfo, ColumnInfo columnInfo, byte[] message)
        {
            if (message == null || message.Length == 0) return null;

            if (entityPropertyInfo.PropertyType.GetTypeInfo().IsEnum)
            {
                foreach (var fInfo in entityPropertyInfo.PropertyType.GetFields(BindingFlags.Public | BindingFlags.Static))
                {
                    var underlyingType = Enum.GetUnderlyingType(entityPropertyInfo.PropertyType);
                    var stringValue = Encoding.Unicode.GetString(message);
                    var value = Convert.ChangeType(stringValue, underlyingType);
                    var enumVal = fInfo.GetRawConstantValue();
                    if (value == enumVal) return enumVal;
                }
            }

            if (entityPropertyInfo.PropertyType == typeof(byte[])) return message;

            if (entityPropertyInfo.PropertyType == typeof(bool) || entityPropertyInfo.PropertyType == typeof(bool?)) return Encoding.Unicode.GetString(message).ToBoolean();

            if (entityPropertyInfo.PropertyType == typeof(char[])) return Encoding.Unicode.GetString(message).ToCharArray();

            return base.GetValue(entityPropertyInfo, columnInfo, message);
        }
    }
}