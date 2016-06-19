#region License
// TableDependency, SqlTableDependency, OracleTableDependency
// Copyright (c) 2015-2106 Christian Del Bianco. All rights reserved.
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
using System.Globalization;
using System.Linq;
using System.Reflection;
using Oracle.ManagedDataAccess.Types;
using TableDependency.Classes;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.Messages;
using TableDependency.OracleClient.Helpers;

namespace TableDependency.OracleClient.EventArgs
{
    public sealed class OracleRecordChangedEventArgs<T> : RecordChangedEventArgs<T> where T : class
    {
        #region Instance variables

        private DateTimeStampWithLocalTimeZoneFormat _dateTimeStampWithLocalTimeZoneFormat = new DateTimeStampWithLocalTimeZoneFormat();
        private DateTimeStampWithTimeZoneFormat _dateTimeStampWithTimeZoneFormat = new DateTimeStampWithTimeZoneFormat();
        private DateStampFormat _dateStampFormat = new DateStampFormat();
        private DateFormat _dateFormat = new DateFormat();
        private const string QUOTES = "\"";

        #endregion

        #region Constructors

        internal OracleRecordChangedEventArgs(MessagesBag messagesBag, ModelToTableMapper<T> mapper, IEnumerable<ColumnInfo> userInterestedColumns) : base(messagesBag, mapper, userInterestedColumns)
        {
        }

        #endregion

        #region Internal methods

        internal override ColumnInfo GetColumnInfo(string columnName)
        {
            return UserInterestedColumns.First(uic => string.Equals(uic.Name.Replace(QUOTES, ""), columnName, StringComparison.CurrentCultureIgnoreCase));
        }

        /// <remarks>
        /// .NET DateTime structure has a precision of tick - 100 nanoseconds - 0.0000001 of second - 7 decimal positions after the point.
        /// Oracle TimeStamp has a precision of up to nanosecond - 0.000000001 - 9 decimal positions after the point.
        /// 
        /// The length of a CHAR column is fixed to the length that you declare when you create the table.
        /// When CHAR values are stored, they are right-padded with spaces to the specified length. 
        /// When CHAR values are retrieved, trailing spaces are removed unless the PAD_CHAR_TO_FULL_LENGTH SQL mode is enabled. 
        /// 
        /// Datetime Datatypes:
        /// - DATE
        /// - TIMESTAMP
        /// - TIMESTAMP WITH TIME ZONE
        /// - TIMESTAMP WITH LOCAL TIME ZONE
        /// </remarks>
        /// <see cref="http://msdn.microsoft.com/en-us/library/yk72thhd%28v=vs.110%29.aspx?f=255&MSPPError=-2147217396"/>
        /// <see cref="http://docs.oracle.com/cd/B19306_01/server.102/b14225/ch4datetime.htm#i1006169"/>
        /// <see cref="http://msdn.microsoft.com/en-us/library/8kb3ddd4%28v=vs.110%29.aspx"/>
        internal override object GetValue(PropertyInfo entityPropertyInfo, ColumnInfo columnInfo, byte[] message)
        {
            if (message == null || message.Length == 0) return null;

            var stringValue = this.MessagesBag.Encoding.GetString(message).ToString(CultureInfo.CurrentCulture);
            if (string.IsNullOrWhiteSpace(stringValue)) return entityPropertyInfo.GetType().IsValueType ? Activator.CreateInstance(entityPropertyInfo.GetType()) : null;

            if (columnInfo.Type == "DATE")
            {
                DateTime result;
                return DateTime.TryParseExact(stringValue, _dateFormat.NetFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out result) ? result : default(DateTime);
            }

            if (columnInfo.Type.EndsWith("WITH TIME ZONE"))
            {
                DateTimeOffset result;
                stringValue = this.NormalizeNumberOfMilliseconds(stringValue, _dateTimeStampWithTimeZoneFormat.NetTimeZoneFormat);
                return DateTimeOffset.TryParseExact(stringValue, _dateTimeStampWithTimeZoneFormat.NetFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out result) ? result : default(DateTimeOffset);
            }

            if (columnInfo.Type.EndsWith("WITH LOCAL TIME ZONE"))
            {
                DateTime result;
                stringValue = this.NormalizeNumberOfMilliseconds(stringValue);
                return DateTime.TryParseExact(stringValue, _dateTimeStampWithLocalTimeZoneFormat.NetFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out result) ? result : default(DateTime);
            }

            if (columnInfo.Type.StartsWith("INTERVAL YEAR"))
            {
                return new OracleIntervalYM(stringValue).Value;
            }

            if (columnInfo.Type.StartsWith("INTERVAL DAY"))
            {
                int tempInt;
                var dts = (OracleIntervalDS)stringValue;
                tempInt = int.TryParse(dts.Milliseconds.ToString("000").Substring(0, 3), out tempInt) ? tempInt : 0;
                return new TimeSpan(dts.Days, dts.Hours, dts.Minutes, dts.Seconds, tempInt);
            }

            if (columnInfo.Type.StartsWith("TIMESTAMP"))
            {
                DateTime result;
                stringValue = this.NormalizeNumberOfMilliseconds(stringValue);
                return DateTime.TryParseExact(stringValue, _dateStampFormat.NetFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out result) ? result : default(DateTime);
            }

            if (columnInfo.Type.StartsWith("XMLTYPE"))
            {
                return stringValue;
            }

            if (columnInfo.Type.StartsWith("RAW"))
            {
                return message;
            }

            if (columnInfo.Type == "NCHAR" || columnInfo.Type == "CHAR")
            {
                return stringValue.ToCharArray();
            }

            //value = TypeDescriptor
            //    .GetConverter(entityPropertyInfo.PropertyType)
            //    .ConvertFromString(null, culture: CultureInfo.CurrentCulture, text: this.MessagesBag.Encoding.GetString(message).ToString(CultureInfo.CurrentCulture));

            return base.GetValue(entityPropertyInfo, columnInfo, message);
        }

        private string NormalizeNumberOfMilliseconds(string value, string timeZone = null)
        {
            var dateTimeStamp = value.Trim();
            var timeZoneValue = "+00:00";

            if (!string.IsNullOrWhiteSpace(timeZone))
            {
                var signIndex = value.IndexOf('+');
                if (signIndex != -1)
                {
                    timeZoneValue = value.Substring(signIndex).Trim();
                    dateTimeStamp = value.Replace(timeZoneValue, string.Empty).Trim();
                }

                signIndex = value.IndexOf('-');
                if (signIndex != -1)
                {
                    timeZoneValue = value.Substring(signIndex).Trim();
                    dateTimeStamp = value.Replace(timeZoneValue, string.Empty).Trim();
                }
            }

            var numberOfAdmittedMilliseconds = _dateStampFormat.NetFormat.Count(c => c == 'f');
            var index = dateTimeStamp.LastIndexOf('.');
            if (index != -1)
            {
                var numberOfMilliseconds = dateTimeStamp.Substring(index).Trim().Length;
                if (numberOfMilliseconds != numberOfAdmittedMilliseconds)
                {
                    if (numberOfAdmittedMilliseconds < numberOfMilliseconds)
                    {
                        dateTimeStamp = dateTimeStamp.Substring(0, index + 1 + numberOfAdmittedMilliseconds);
                    }
                    else if (numberOfAdmittedMilliseconds > numberOfMilliseconds)
                    {
                        dateTimeStamp += new string('0', (numberOfAdmittedMilliseconds - numberOfMilliseconds) + 1);
                    }
                }
            }
            else
            {
                dateTimeStamp += "." + new string('0', numberOfAdmittedMilliseconds);
            }

            return (string.IsNullOrWhiteSpace(timeZone)) ? dateTimeStamp : dateTimeStamp + " " + timeZoneValue;
        }

        #endregion
    }
}