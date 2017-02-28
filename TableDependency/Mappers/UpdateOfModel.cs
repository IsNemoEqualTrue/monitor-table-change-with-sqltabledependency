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
using System.Linq.Expressions;
using System.Reflection;
using TableDependency.Exceptions;

namespace TableDependency.Mappers
{
    public class UpdateOfModel<T> where T : class
    {
        private readonly List<PropertyInfo> _updateOfList = new List<PropertyInfo>();

        /// <summary>
        /// Adds a property name used to specifying which table column must be monitored for changes.
        /// </summary>
        /// <param name="expressions">The expressions.</param>
        public void Add(params Expression<Func<T, object>>[] expressions)
        {
            if (expressions != null && expressions.Length > 0)
            {
                foreach (var expression in expressions)
                {
                    var memberExpression = expression.Body as MemberExpression;
                    if (memberExpression != null)
                    {
                        _updateOfList.Add((PropertyInfo)memberExpression.Member);
                    }
                    else
                    {
                        var unarUnaryExpressionyExp = expression.Body as UnaryExpression;
                        var memberExpressionByOperator = unarUnaryExpressionyExp?.Operand as MemberExpression;
                        if (memberExpressionByOperator != null)
                        {
                            _updateOfList.Add((PropertyInfo)memberExpressionByOperator.Member);
                        }
                        else
                        {
                            throw new UpdateOfModelException("'expression' parameter should be a member expression.");
                        }
                    }
                }
            }
            else
            {
                throw new UpdateOfModelException("UpdateOfModel cannot be empty.");
            }
        }

        #region Internal methods

        internal int Count()
        {
            return _updateOfList.Count;
        }

        internal IList<PropertyInfo> GetPropertiesInfos()
        {
            return _updateOfList;
        }
    }

    #endregion
}