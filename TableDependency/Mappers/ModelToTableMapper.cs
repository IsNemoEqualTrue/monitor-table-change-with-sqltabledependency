////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using TableDependency.Exceptions;

namespace TableDependency.Mappers
{
    /// <summary>
    /// Model to column database table mapper.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class ModelToTableMapper<T> where T : class
    {
        private readonly IDictionary<PropertyInfo, string> _mappings = new Dictionary<PropertyInfo, string>();

        /// <summary>
        /// Adds the mapping between a model property and a database table column, in order to decouple naming and to overcome the impossibility to map SQL columns name containing spaces.
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <param name="columnName">Name of the column.</param>
        /// <returns></returns>
        public ModelToTableMapper<T> AddMapping(Expression<Func<T, object>> expression, string columnName)
        {
            if (string.IsNullOrWhiteSpace(columnName)) throw new ModelToTableMapperException("ModelToTableMapper cannot contains null or empty strings.");

            var memberExpression = expression.Body as MemberExpression;
            if (memberExpression != null)
            {
                _mappings[(PropertyInfo)memberExpression.Member] = columnName;
                return this;
            }

            var unarUnaryExpressionyExp = expression.Body as UnaryExpression;
            var memberExpressionByOperator = unarUnaryExpressionyExp?.Operand as MemberExpression;
            if (memberExpressionByOperator != null)
            {
                _mappings[(PropertyInfo)memberExpressionByOperator.Member] = columnName;
                return this;
            }

            throw new TableDependencyException("'expression' parameter should be a member expression");
        }

        /// <summary>
        /// Return number of mappings.
        /// </summary>
        /// <returns></returns>
        public int Count()
        {
            return _mappings.Count;
        }

        #region Internal methods

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

        internal void AddMapping(PropertyInfo pi, string columnName)
        {
            _mappings[(PropertyInfo)pi] = columnName;
        }

        internal IDictionary<PropertyInfo, string> GetMappings()
        {
            return _mappings;
        }

        #endregion
    }
}