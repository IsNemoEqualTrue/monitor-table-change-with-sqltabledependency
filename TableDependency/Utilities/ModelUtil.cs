////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   © 2015-2106 Christian Del Bianco. All rights reserved.
////////////////////////////////////////////////////////////////////////////////

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace TableDependency.Utilities
{
    internal static class ModelUtil
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
            typeof (Guid)
        };

        internal static IEnumerable<PropertyInfo> GetModelPropertiesInfo<T>()
        {
            return typeof(T)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.SetField)
                .Where(propertyInfo => ProcessableModelTypes.Contains(propertyInfo.PropertyType) || (propertyInfo.PropertyType.IsGenericType && propertyInfo.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>)))
                .ToArray();
        }
    }
}