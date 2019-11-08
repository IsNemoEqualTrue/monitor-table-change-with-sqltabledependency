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
#endregion

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

using TableDependency.SqlClient.Base.Abstracts;
using TableDependency.SqlClient.Base.Exceptions;

namespace TableDependency.SqlClient.Base
{
    /// <summary>
    /// Model to column database table mapper.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class ModelToTableMapper<T> : IModelToTableMapper<T> where T : class
    {
        #region Instance variables

        private readonly IDictionary<PropertyInfo, string> _mappings = new Dictionary<PropertyInfo, string>();

        #endregion

        #region Public methods

        /// <summary>
        /// Adds the mapping.
        /// </summary>
        /// <param name="pi">The pi.</param>
        /// <param name="columnName">Name of the column.</param>
        public ModelToTableMapper<T> AddMapping(PropertyInfo pi, string columnName)
        {
            if (_mappings.Values.Any(cn => cn == columnName))
            {
                throw new ModelToTableMapperException("Duplicate mapping for column " + columnName);
            }

            _mappings[pi] = columnName;

            return this;
        }

        /// <summary>
        /// Adds the mapping between a model property and a database table column.
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <param name="columnName">Name of the column.</param>
        /// <returns></returns>
        public ModelToTableMapper<T> AddMapping(Expression<Func<T, object>> expression, string columnName)
        {
            if (string.IsNullOrWhiteSpace(columnName)) throw new ModelToTableMapperException("ModelToTableMapper cannot contains null or empty strings.");

            if (expression.Body is MemberExpression memberExpression)
            {
                _mappings[(PropertyInfo)memberExpression.Member] = columnName;
                return this;
            }

            var unaryExpression = expression.Body as UnaryExpression;
            if (unaryExpression?.Operand is MemberExpression memberExpressionByOperator)
            {
                _mappings[(PropertyInfo)memberExpressionByOperator.Member] = columnName;
                return this;
            }

            throw new UpdateOfModelException("The 'expression' parameter should be a member expression.");
        }

        /// <summary>
        /// Return number of mappings.
        /// </summary>
        /// <returns></returns>
        public int Count()
        {
            return _mappings.Count;
        }

        /// <summary>
        /// Gets the mapping.
        /// </summary>
        /// <param name="propertyInfo">The property information.</param>
        /// <returns></returns>
        public string GetMapping(PropertyInfo propertyInfo)
        {
            return _mappings.Any(p => p.Key.Name == propertyInfo.Name) ? _mappings.First(p => p.Key.Name == propertyInfo.Name).Value : null;
        }

        /// <summary>
        /// Gets the mappings.
        /// </summary>
        /// <returns></returns>
        public IDictionary<PropertyInfo, string> GetMappings()
        {
            return _mappings;
        }

        /// <summary>
        /// Gets the mapping.
        /// </summary>
        /// <param name="tableColumnName">Name of the table column.</param>
        /// <returns></returns>
        public string GetMapping(string tableColumnName)
        {
            if (GetMappings().Any(kvp => kvp.Value != null && string.Compare(kvp.Value, tableColumnName, StringComparison.OrdinalIgnoreCase) == 0))
            {
                var mapping = this.GetMappings().First(kvp => string.Compare(kvp.Value, tableColumnName, StringComparison.OrdinalIgnoreCase) == 0);
                return mapping.Value;
            }

            return null;
        }

        #endregion
    }
}