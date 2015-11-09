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
using Oracle.DataAccess.Types;
using TableDependency.Classes;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.Messages;

namespace TableDependency.OracleClient.EventArgs
{
    public sealed class OracleRecordChangedEventArgs<T> : RecordChangedEventArgs<T> where T : class
    {
        #region Instance variables

        private const string QUOTES = "\"";
        private const string DATE_FORMAT = "MM-dd-yyyy HH:mm:ss";

        #endregion

        #region Constructors

        internal OracleRecordChangedEventArgs(MessagesBag messagesBag, ModelToTableMapper<T> mapper, IEnumerable<ColumnInfo> userInterestedColumns) : base(messagesBag, mapper, userInterestedColumns)
        {
        }

        #endregion

        #region Internal methods

        internal override ColumnInfo GetColumnInfo(string columnName)
        {
            return base._userInterestedColumns.First(uic => string.Equals(uic.Name.Replace(QUOTES, ""), columnName, StringComparison.CurrentCultureIgnoreCase));
        }

        /// <remarks>
        /// .NET DateTime structure has a precision of tick - 100 nanoseconds - 0.0000001 of second - 7 decimal positions after the point.
        /// Oracle TimeStamp has a precision of up to nanosecond - 0.000000001 - 9 decimal positions after the point.
        /// 
        /// The length of a CHAR column is fixed to the length that you declare when you create the table.
        /// When CHAR values are stored, they are right-padded with spaces to the specified length. 
        /// When CHAR values are retrieved, trailing spaces are removed unless the PAD_CHAR_TO_FULL_LENGTH SQL mode is enabled. 
        /// </remarks>
        internal override object GetValue(PropertyInfo entityPropertyInfo, ColumnInfo columnInfo, byte[] message)
        {
            object value = null;

            if (message != null)
            {
                var stringValue = this.MessagesBag.Encoding.GetString(message).ToString(CultureInfo.CurrentCulture);
                int tempInt;

                // DATE
                if (columnInfo.Type == "DATE")
                {
                    return DateTime.ParseExact(stringValue, DATE_FORMAT, CultureInfo.InvariantCulture);
                }

                // INTERVAL YEAR(n) TO MONTH
                if (columnInfo.Type.StartsWith("INTERVAL YEAR"))
                {
                    return new OracleIntervalYM(stringValue).Value;
                }

                // INTERVAL DAY(n) TO SECOND(n)
                if (columnInfo.Type.StartsWith("INTERVAL DAY"))
                {
                    var dts = (OracleIntervalDS)stringValue;
                    tempInt = int.TryParse(dts.Milliseconds.ToString("000").Substring(0, 3), out tempInt) ? tempInt : 0;
                    return new TimeSpan(dts.Days, dts.Hours, dts.Minutes, dts.Seconds, tempInt);
                }

                // TIMESTAMP(n) WITH LOCAL TIME ZONE
                if (columnInfo.Type.EndsWith("WITH TIME ZONE"))
                {
                    var ots = (OracleTimeStampTZ)stringValue;
                    tempInt = int.TryParse(ots.Millisecond.ToString("000").Substring(0, 3), out tempInt) ? tempInt : 0;
                    return new DateTime(ots.Year, ots.Month, ots.Day, ots.Hour, ots.Minute, ots.Second, tempInt);
                }

                // TIMESTAMP(n) WITH TIME ZONE
                if (columnInfo.Type.EndsWith("WITH LOCAL TIME ZONE"))                    
                {
                    var ots = (OracleTimeStampLTZ)stringValue;
                    tempInt = int.TryParse(ots.Millisecond.ToString("000").Substring(0, 3), out tempInt) ? tempInt : 0;
                    return new DateTime(ots.Year, ots.Month, ots.Day, ots.Hour, ots.Minute, ots.Second, tempInt);
                }

                // TIMESTAMP(n)
                if (columnInfo.Type.StartsWith("TIMESTAMP"))
                {
                    var ots = (OracleTimeStamp)stringValue;
                    tempInt = int.TryParse(ots.Millisecond.ToString("000").Substring(0, 3), out tempInt) ? tempInt : 0;
                    return new DateTime(ots.Year, ots.Month, ots.Day, ots.Hour, ots.Minute, ots.Second, tempInt);
                }

                // XMLTYPE
                if (columnInfo.Type.StartsWith("XMLTYPE"))
                {
                    return stringValue;
                }
                // RAW
                if(columnInfo.Type.StartsWith("RAW"))
                {
                    return message;
                }

                // NCHAR & CHAR
                if (columnInfo.Type == "NCHAR" || columnInfo.Type == "CHAR")
                {
                    return stringValue.ToCharArray();
                }

                value = TypeDescriptor
                    .GetConverter(entityPropertyInfo.PropertyType)
                    .ConvertFromString(null, culture: CultureInfo.CurrentCulture, text: this.MessagesBag.Encoding.GetString(message).ToString(CultureInfo.CurrentCulture));
            }

            return value;
        }

        #endregion
    }
}