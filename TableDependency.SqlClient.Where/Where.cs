#region License
// TableDependency, SqlTableDependency, OracleTableDependency
// Copyright (c) 2015-2106 Christian Del Bianco. All rights reserved.
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
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace TableDependency.SqlClient.Where
{
    public class Where : ExpressionVisitor
    {
        private readonly StringBuilder _whereConditionBuilder = new StringBuilder();

        public int? Skip { get; private set; } = null;

        public int? Take { get; private set; } = null;

        public string OrderBy { get; private set; } = string.Empty;

        public string Translate(Expression expression)
        {
            this.Visit(expression);
            return _whereConditionBuilder.ToString();
        }

        private static Expression StripQuotes(Expression e)
        {
            while (e.NodeType == ExpressionType.Quote)
            {
                e = ((UnaryExpression)e).Operand;
            }

            return e;
        }

        protected override Expression VisitMethodCall(MethodCallExpression m)
        {
            if (m.Method.DeclaringType == typeof(Queryable) && m.Method.Name == "Where")
            {
                this.Visit(m.Arguments[0]);
                var lambda = (LambdaExpression)StripQuotes(m.Arguments[1]);
                this.Visit(lambda.Body);
                return m;
            }

            #region StartsWith

            if (m.Method.Name == "StartsWith")
            {
                var memberExpression = m.Object as MemberExpression;
                if (memberExpression == null) return Expression.Empty();

                this.VisitMember((MemberExpression)m.Object);
                _whereConditionBuilder.Append(" LIKE ");

                var containsParameter = (ConstantExpression)m.Arguments[0];
                var value = containsParameter?.Value as string;
                if (string.IsNullOrWhiteSpace(value)) Expression.Empty();

                var likeParameter = value + "%";
                var nextExpression = Expression.Constant(likeParameter, typeof(string));
                var expression = this.Visit(nextExpression);

                return expression;
            }

            #endregion

            #region EndsWith

            if (m.Method.Name == "EndsWith")
            {
                var memberExpression = m.Object as MemberExpression;
                if (memberExpression == null) return Expression.Empty();

                this.VisitMember((MemberExpression)m.Object);
                _whereConditionBuilder.Append(" LIKE ");

                var containsParameter = (ConstantExpression)m.Arguments[0];
                var value = containsParameter?.Value as string;
                if (string.IsNullOrWhiteSpace(value)) Expression.Empty();

                var likeParameter = "%" + value;
                var nextExpression = Expression.Constant(likeParameter, typeof(string));
                var expression = this.Visit(nextExpression);

                return expression;
            }

            #endregion

            #region Contains

            if (m.Method.Name == "Contains")
            {
                var memberExpression = m.Object as MemberExpression;
                if (memberExpression == null) return Expression.Empty();

                this.VisitMember((MemberExpression)m.Object);
                _whereConditionBuilder.Append(" LIKE ");

                var containsParameter = (ConstantExpression)m.Arguments[0];
                var value = containsParameter?.Value as string;
                if (string.IsNullOrWhiteSpace(value)) Expression.Empty();

                var likeParameter = "%" + value + "%";
                var nextExpression = Expression.Constant(likeParameter, typeof(string));
                var expression = this.Visit(nextExpression);

                return expression;
            }

            #endregion

            #region Trim

            if (m.Method.Name == "Trim")
            {
                var memberExpression = m.Object as MemberExpression;
                if (memberExpression == null) return Expression.Empty();

                _whereConditionBuilder.Append("LTRIM(RTRIM(");
                this.VisitMember((MemberExpression)m.Object);
                _whereConditionBuilder.Append("))");

                return Expression.Empty();
            }

            #endregion

            #region TrimStart

            if (m.Method.Name == "TrimStart")
            {
                var memberExpression = m.Object as MemberExpression;
                if (memberExpression == null) return Expression.Empty();

                _whereConditionBuilder.Append("LTRIM(");
                this.VisitMember((MemberExpression)m.Object);
                _whereConditionBuilder.Append(")");

                return Expression.Empty();
            }

            #endregion

            #region TrimEnd

            if (m.Method.Name == "TrimEnd")
            {
                var memberExpression = m.Object as MemberExpression;
                if (memberExpression == null) return Expression.Empty();

                _whereConditionBuilder.Append("RTRIM(");
                this.VisitMember((MemberExpression)m.Object);
                _whereConditionBuilder.Append(")");

                return Expression.Empty();
            }

            #endregion

            #region ToUpper

            if (m.Method.Name == "ToUpper")
            {
                var memberExpression = m.Object as MemberExpression;
                if (memberExpression == null) return Expression.Empty();

                _whereConditionBuilder.Append("UPPER(");
                this.VisitMember((MemberExpression)m.Object);
                _whereConditionBuilder.Append(")");

                return Expression.Empty();
            }

            #endregion

            #region ToLower

            if (m.Method.Name == "ToLower")
            {
                var memberExpression = m.Object as MemberExpression;
                if (memberExpression == null) return Expression.Empty();

                _whereConditionBuilder.Append("LOWER(");
                this.VisitMember((MemberExpression)m.Object);
                _whereConditionBuilder.Append(")");

                return Expression.Empty();
            }

            #endregion

            #region Substring

            if (m.Method.Name == "Substring")
            {
                int intResult;

                var memberExpression = m.Object as MemberExpression;
                if (memberExpression == null) return Expression.Empty();

                _whereConditionBuilder.Append("SUBSTRING (");
                this.VisitMember((MemberExpression)m.Object);

                var startParameter = (ConstantExpression)m.Arguments[0];
                if (!int.TryParse(startParameter?.Value.ToString(), out intResult)) Expression.Empty();
                _whereConditionBuilder.Append(", " + intResult);

                var lenParameter = (ConstantExpression)m.Arguments[1];
                if (!int.TryParse(lenParameter?.Value.ToString(), out intResult)) Expression.Empty();
                _whereConditionBuilder.Append(", " + intResult + ")");

                return Expression.Empty();
            }

            #endregion

            #region ToString

            if (m.Method.Name == "ToString")
            {
                var memberExpression = m.Object as MemberExpression;
                if (memberExpression == null) return Expression.Empty();

                _whereConditionBuilder.Append("CONVERT(varchar(MAX), ");
                this.VisitMember((MemberExpression)m.Object);
                _whereConditionBuilder.Append(")");

                return Expression.Empty();
            }

            #endregion

            //else if (m.Method.Name == "Take")
            //{
            //    if (this.ParseTakeExpression(m))
            //    {
            //        var nextExpression = m.Arguments[0];
            //        return this.Visit(nextExpression);
            //    }
            //}
            //else if (m.Method.Name == "OrderBy")
            //{
            //    if (this.ParseOrderByExpression(m, "ASC"))
            //    {
            //        var nextExpression = m.Arguments[0];
            //        return this.Visit(nextExpression);
            //    }
            //}
            //else if (m.Method.Name == "OrderByDescending")
            //{
            //    if (this.ParseOrderByExpression(m, "DESC"))
            //    {
            //        var nextExpression = m.Arguments[0];
            //        return this.Visit(nextExpression);
            //    }
            //}

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
            if (m.Member.Name == "Length" && m.Expression.NodeType == ExpressionType.MemberAccess)
            {
                var memberExpression = m.Expression as MemberExpression;
                if (memberExpression == null) return Expression.Empty();

                _whereConditionBuilder.Append("LEN(" + memberExpression.Member.Name + ")");
                return m;
            }

            if (m.Expression == null || m.Expression.NodeType != ExpressionType.Parameter)
            {
                throw new NotSupportedException($"The member '{m.Member.Name}' is not supported");
            }

            _whereConditionBuilder.Append(m.Member.Name);

            return m;
        }

        protected bool IsNullConstant(Expression exp)
        {
            return (exp.NodeType == ExpressionType.Constant && ((ConstantExpression)exp).Value == null);
        }

        private bool ParseOrderByExpression(MethodCallExpression expression, string order)
        {
            var unary = (UnaryExpression)expression.Arguments[1];
            var lambdaExpression = (LambdaExpression)unary.Operand;

            lambdaExpression = (LambdaExpression)Evaluator.PartialEval(lambdaExpression);

            var body = lambdaExpression.Body as MemberExpression;
            if (body == null) return false;

            this.OrderBy = string.IsNullOrEmpty(OrderBy) ? $"{body.Member.Name} {order}" : $"{OrderBy}, {body.Member.Name} {order}";
            return true;
        }

        private bool ParseTakeExpression(MethodCallExpression expression)
        {
            var sizeExpression = (ConstantExpression)expression.Arguments[1];

            int size;
            if (!int.TryParse(sizeExpression.Value.ToString(), out size)) return false;
            this.Take = size;
            return true;
        }
    }
}