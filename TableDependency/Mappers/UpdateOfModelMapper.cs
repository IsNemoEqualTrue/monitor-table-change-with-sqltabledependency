////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco. All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace TableDependency.Mappers
{
    public class UpdateOfModelMapper<T> where T : class
    {
        private readonly List<PropertyInfo> _updateOfList = new List<PropertyInfo>();

        public void AddMappssssing(params Expression<Func<T, object>>[] expressions)
        {
            foreach (var expression in expressions)
            {
                var memberExpression = expression.Body as MemberExpression;
                if (memberExpression != null)
                {
                    _updateOfList.Add((PropertyInfo) memberExpression.Member);
                }
                else
                {
                    var unarUnaryExpressionyExp = expression.Body as UnaryExpression;
                    var memberExpressionByOperator = unarUnaryExpressionyExp?.Operand as MemberExpression;
                    if (memberExpressionByOperator != null)
                    {
                        _updateOfList.Add((PropertyInfo) memberExpressionByOperator.Member);
                    }
                }

                throw new ArgumentException("'expression' parameter should be a member expression", nameof(expression));
            }
        }

        public int Count()
        {
            return _updateOfList.Count;
        }

        #region Internal methods

        internal IList<PropertyInfo> GetMappings()
        {
            return _updateOfList;
        }
    }

    #endregion
}