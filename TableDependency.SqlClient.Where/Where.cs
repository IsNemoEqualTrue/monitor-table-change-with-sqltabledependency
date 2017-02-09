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
using System.CodeDom;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;

namespace TableDependency.SqlClient.Where
{
    public class Where : ExpressionVisitor
    {
        private readonly StringBuilder _whereConditionBuilder = new StringBuilder();

        public string Translate(Expression expression)
        {
            this.Visit(expression);
            return _whereConditionBuilder.ToString();
        }


        #region Protected Methods 

        protected override Expression VisitInvocation(InvocationExpression e)
        {
            return null;
        }

        protected override Expression VisitMethodCall(MethodCallExpression m)
        {

            //if (this.IsLinqOperator(m.Method))
            //{
            //    throw new ArgumentException();
            //}

            //if (m.Method.DeclaringType == typeof(Queryable) && m.Method.Name == "Where")
            //{
            //    throw new ArgumentException("Cosa fai? ridefinisci una where per la seconda volta ?");
            //}

            //var methodCallExpression = m.Object as MethodCallExpression;
            //if (methodCallExpression != null) this.Visit(methodCallExpression);

            if (m.Method.Name == "StartsWith")
            {
                this.VisitMember((MemberExpression)m.Object);
                _whereConditionBuilder.Append(" LIKE ");

                var containsParameter = (ConstantExpression)m.Arguments[0];
                var value = containsParameter?.Value as string;
                if (string.IsNullOrWhiteSpace(value)) Expression.Empty();

                var likeParameter = value + "%";
                var nextExpression = Expression.Constant(likeParameter, typeof(string));
                var expression = this.Visit(nextExpression);

                return m;
            }

            if (m.Method.Name == "EndsWith")
            {
                this.VisitMember((MemberExpression)m.Object);
                _whereConditionBuilder.Append(" LIKE ");

                var containsParameter = (ConstantExpression)m.Arguments[0];
                var value = containsParameter?.Value as string;
                if (string.IsNullOrWhiteSpace(value)) Expression.Empty();

                var likeParameter = "%" + value;
                var nextExpression = Expression.Constant(likeParameter, typeof(string));
                var expression = this.Visit(nextExpression);

                return m;
            }

            if (m.Method.Name == "Contains")
            {
                this.VisitMember((MemberExpression)m.Object);
                _whereConditionBuilder.Append(" LIKE ");

                var containsParameter = (ConstantExpression)m.Arguments[0];
                var value = containsParameter?.Value as string;
                if (string.IsNullOrWhiteSpace(value)) Expression.Empty();

                var likeParameter = "%" + value + "%";
                var nextExpression = Expression.Constant(likeParameter, typeof(string));
                var expression = this.Visit(nextExpression);

                return m;
            }

            if (m.Method.Name == "Trim")
            {
                _whereConditionBuilder.Append("LTRIM(RTRIM(");
                this.VisitMember((MemberExpression)m.Object);
                _whereConditionBuilder.Append("))");

                return m;
            }

            if (m.Method.Name == "TrimStart")
            {
                _whereConditionBuilder.Insert(0, "LTRIM(");

                this.VisitMember((MemberExpression)m.Object);
                _whereConditionBuilder.Append(")");

                return m;
            }

            if (m.Method.Name == "TrimEnd")
            {
                _whereConditionBuilder.Append("RTRIM(");
                this.VisitMember((MemberExpression)m.Object);
                _whereConditionBuilder.Append(")");

                return m;
            }

            if (m.Method.Name == "ToUpper")
            {
                _whereConditionBuilder.Append("UPPER(");
                this.VisitMember((MemberExpression)m.Object);
                _whereConditionBuilder.Append(")");

                return m;
            }

            if (m.Method.Name == "ToLower")
            {
                _whereConditionBuilder.Append("LOWER(");
                this.VisitMember((MemberExpression)m.Object);
                _whereConditionBuilder.Append(")");

                return m;
            }

            if (m.Method.Name == "Substring")
            {
                int intResult;

                _whereConditionBuilder.Append("SUBSTRING (");
                this.VisitMember((MemberExpression)m.Object);

                var startParameter = (ConstantExpression)m.Arguments[0];
                if (!int.TryParse(startParameter?.Value.ToString(), out intResult)) Expression.Empty();
                _whereConditionBuilder.Append(", " + intResult);

                var lenParameter = (ConstantExpression)m.Arguments[1];
                if (!int.TryParse(lenParameter?.Value.ToString(), out intResult)) Expression.Empty();
                _whereConditionBuilder.Append(", " + intResult + ")");

                return m;
            }

            if (m.Method.Name == "ToString")
            {
                _whereConditionBuilder.Append("CONVERT(varchar(MAX), ");
                this.VisitMember((MemberExpression)m.Object);
                _whereConditionBuilder.Append(")");

                return m;
            }

            throw new NotSupportedException($"The method '{m.Method.Name}' is not supported");
        }

        protected override Expression VisitUnary(UnaryExpression u)
        {
            switch (u.NodeType)
            {
                case ExpressionType.Not:
                    _whereConditionBuilder.Append(" NOT ");
                    this.Visit(u.Operand);
                    break;

                case ExpressionType.Convert:
                    this.Visit(u.Operand);
                    break;

                default:
                    throw new NotSupportedException($"The unary operator '{u.NodeType}' is not supported");
            }

            return u;
        }

        protected override Expression VisitBinary(BinaryExpression b)
        {
            this.Visit(b.Left);

            switch (b.NodeType)
            {
                case ExpressionType.And:
                    _whereConditionBuilder.Append(" AND ");
                    break;

                case ExpressionType.AndAlso:
                    _whereConditionBuilder.Append(" AND ");
                    break;

                case ExpressionType.Or:
                    _whereConditionBuilder.Append(" OR ");
                    break;

                case ExpressionType.OrElse:
                    _whereConditionBuilder.Append(" OR ");
                    break;

                case ExpressionType.Equal:
                    _whereConditionBuilder.Append(IsNullConstant(b.Right) ? " IS " : " = ");
                    break;

                case ExpressionType.NotEqual:
                    _whereConditionBuilder.Append(IsNullConstant(b.Right) ? " IS NOT " : " <> ");
                    break;

                case ExpressionType.LessThan:
                    _whereConditionBuilder.Append(" < ");
                    break;

                case ExpressionType.LessThanOrEqual:
                    _whereConditionBuilder.Append(" <= ");
                    break;

                case ExpressionType.GreaterThan:
                    _whereConditionBuilder.Append(" > ");
                    break;

                case ExpressionType.GreaterThanOrEqual:
                    _whereConditionBuilder.Append(" >= ");
                    break;

                default:
                    throw new NotSupportedException($"The binary operator '{b.NodeType}' is not supported");
            }

            this.Visit(b.Right);

            return b;
        }

        protected override Expression VisitConstant(ConstantExpression c)
        {
            var q = c.Value as IQueryable;
            if (q == null && c.Value == null)
            {
                _whereConditionBuilder.Append("NULL");
            }
            else if (q == null)
            {
                switch (Type.GetTypeCode(c.Value.GetType()))
                {
                    case TypeCode.Boolean:
                        _whereConditionBuilder.Append((bool)c.Value ? 1 : 0);
                        break;

                    case TypeCode.String:
                        _whereConditionBuilder.Append("'");
                        _whereConditionBuilder.Append(c.Value);
                        _whereConditionBuilder.Append("'");
                        break;

                    case TypeCode.DateTime:
                        _whereConditionBuilder.Append("'");
                        _whereConditionBuilder.Append(c.Value);
                        _whereConditionBuilder.Append("'");
                        break;

                    case TypeCode.Object:
                        throw new NotSupportedException($"The constant for '{c.Value}' is not supported");

                    default:
                        _whereConditionBuilder.Append(c.Value);
                        break;
                }
            }

            return c;
        }

        protected override Expression VisitMember(MemberExpression m)
        {
            if (m.Expression.NodeType == ExpressionType.Call)
            {
                this.Visit(m.Expression);
            }

            
            //methodCallExpressionif

            _whereConditionBuilder.Append(m.Member.Name);

            return m;
        }

        #endregion

        #region Private Methods

        private bool IsLinqOperator(MethodInfo method)
        {
            if (method.DeclaringType != typeof(Queryable) && method.DeclaringType != typeof(Enumerable)) return false;
            return Attribute.GetCustomAttribute(method, typeof(ExtensionAttribute)) != null;
        }

        private Expression StripQuotes(Expression e)
        {
            while (e.NodeType == ExpressionType.Quote)
            {
                e = ((UnaryExpression)e).Operand;
            }

            return e;
        }

        private bool IsNullConstant(Expression exp)
        {
            return exp.NodeType == ExpressionType.Constant && ((ConstantExpression)exp).Value == null;
        }

        #endregion
    }
}