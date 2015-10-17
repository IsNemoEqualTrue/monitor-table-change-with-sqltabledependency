////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco. All rights reserved.
////////////////////////////////////////////////////////////////////////////////
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
                            throw new TableDependencyException("'expression' parameter should be a member expression");
                        }
                    }
                }
            }
            else
            {
                throw new TableDependencyException("UpdateOfModel cannot be empty");
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