////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;
using System.ComponentModel;
using System.Globalization;

namespace TableDependency.SqlClient.TypeConverters
{
    internal class SqlBooleanConverter : TypeConverter
    {
        public override object ConvertFrom(ITypeDescriptorContext context, CultureInfo culture, object value)
        {
            if (string.IsNullOrWhiteSpace(value?.ToString()))
            {
                return false;
            }

            bool result;
            if (!bool.TryParse(value.ToString(), out result))
            {
                switch (value.ToString())
                {
                    case "1":
                        return true;
                    case "0":
                        return false;
                }
                throw new InvalidCastException();    
            }

            return result;
        }
    }
}