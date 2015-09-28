////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace TableDependency.Utilities
{
    internal static class ModelUtil
    {
        private static readonly IList<Type> _types = new List<Type>()
        {
            typeof (string),
            typeof (short), typeof (short?),
            typeof (int), typeof (int?),
            typeof (long), typeof (long?),
            typeof (decimal), typeof (decimal?),
            typeof (float), typeof (float?),
            typeof (DateTime), typeof (DateTime?),
            typeof (double), typeof (double?),
            typeof (bool), typeof (bool?)
        };

        internal static IEnumerable<PropertyInfo> GetModelPropertiesInfo<T>()
        {
            return typeof(T)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.SetField)
                .Where(propertyInfo => _types.Contains(propertyInfo.PropertyType) || (propertyInfo.PropertyType.IsGenericType && propertyInfo.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>)))
                .ToArray();
        }
    }
}