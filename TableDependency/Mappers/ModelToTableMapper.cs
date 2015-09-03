using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using TableDependency.Exceptions;

namespace TableDependency.Mappers
{
    public class ModelToTableMapper<T> where T : class
    {
        private readonly IDictionary<PropertyInfo, string> _mappings = new Dictionary<PropertyInfo, string>();

        public ModelToTableMapper<T> AddMapping(Expression<Func<T, object>> property, string columnName)
        {
            if (string.IsNullOrWhiteSpace(columnName)) throw new ModelToTableMapperException("ModelToTableMapper cannot contains null or empty strings.");

            _mappings[(PropertyInfo)((MemberExpression)property.Body).Member] = columnName;
            return this;
        }

        public int Count()
        {
            return _mappings.Count;
        }

        internal string GetMapping(PropertyInfo propertyInfo)
        {
            return _mappings.ContainsKey(propertyInfo) ? _mappings[propertyInfo] : null;
        }

        internal string GetMapping(string tableColumnName)
        {
            if (GetMappings().Any(kvp => kvp.Value != null && string.Compare(kvp.Value, tableColumnName, StringComparison.OrdinalIgnoreCase) == 0))
            {
                var mapping = GetMappings().First(kvp => string.Compare(kvp.Value, tableColumnName, StringComparison.OrdinalIgnoreCase) == 0);
                return mapping.Value;
            }

            return null;
        }

        internal IDictionary<PropertyInfo, string> GetMappings()
        {
            return _mappings;
        }
    }
}