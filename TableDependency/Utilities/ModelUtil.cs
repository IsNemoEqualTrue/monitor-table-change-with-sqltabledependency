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
using System.Linq;
using System.Reflection;

namespace TableDependency.Utilities
{
    public static class ModelUtil
    {
        private static readonly IList<Type> ProcessableModelTypes = new List<Type>
        {
            typeof (string),
            typeof (char),            
            typeof (short), typeof (short?),
            typeof (int), typeof (int?),
            typeof (long), typeof (long?),
            typeof (decimal), typeof (decimal?),
            typeof (float), typeof (float?),
            typeof (DateTime), typeof (DateTime?),
            typeof (DateTimeOffset), typeof (DateTimeOffset?),
            typeof (TimeSpan),
            typeof (double), typeof (double?),
            typeof (bool), typeof (bool?),
            typeof (byte[]),
            typeof (char[]),
            typeof (sbyte),
            typeof (byte),
            typeof (Guid),
            typeof (Enum)
        };

        public static IEnumerable<PropertyInfo> GetModelPropertiesInfo<T>()
        {
            return typeof(T)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.SetField)
                .Where(propertyInfo => ProcessableModelTypes.Contains(propertyInfo.PropertyType) || propertyInfo.PropertyType.IsEnum || (propertyInfo.PropertyType.IsGenericType && propertyInfo.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>)))
                .ToArray();
        }
    }
}