#region License
// TableDependency, SqlTableDependency, SqlTableDependencyFilter
// Copyright (c) 2015-2018 Christian Del Bianco. All rights reserved.
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
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

using TableDependency.Abstracts;
using TableDependency.SqlClient.Where.Helpers;

namespace TableDependency.SqlClient.Where
{
    public class SqlTableDependencyFilter<T> : ExpressionVisitor, ITableDependencyFilter where T : class, new()
    {
        #region Constructors

        private readonly ParameterHelper _parameter = new ParameterHelper();
        private readonly Expression _filter;
        private readonly IDictionary<string, string> _modelMapperDictionary;

        private readonly StringBuilder _whereConditionBuilder = new StringBuilder();
        private readonly IList<Type> _types = new List<Type>()
        {
            typeof (short),
            typeof (short?),
            typeof (int),
            typeof (int?),
            typeof (long),
            typeof (long?),
            typeof (string),
            typeof (decimal),
            typeof (decimal?),
            typeof (float),
            typeof (float?),
            typeof (DateTime),
            typeof (DateTime?),
            typeof (double),
            typeof (double?),
            typeof (bool),
            typeof (bool?)
        };

        #endregion

        #region Constructors
        public SqlTableDependencyFilter(Expression filter, IModelToTableMapper<T> modelMapperDictionary = null)
        {
            _filter = filter;

            _modelMapperDictionary = modelMapperDictionary != null && modelMapperDictionary.Count() > 0
                ? modelMapperDictionary.GetMappings().ToDictionary(kvp => kvp.Key.Name, kvp => kvp.Value)
                : this.CreateModelToTableMapperHelper()?.GetMappings().ToDictionary(kvp => kvp.Key.Name, kvp => kvp.Value);
        }

        #endregion

        #region Public Methods

        public string Translate()
        {
            if (_whereConditionBuilder.Length > 0) return _whereConditionBuilder.ToString().Trim();

            this.Visit(_filter);
            return _whereConditionBuilder.ToString().Trim();
        }

        #endregion

        #region Protected Methods

        protected override Expression VisitMethodCall(MethodCallExpression m)
        {
            if (m.Method.DeclaringType == typeof(Queryable) && m.Method.Name == "Where")
            {
                throw new ArgumentException();
            }

            #region Trim

            if (m.Method.Name == "Trim")
            {
                _whereConditionBuilder.Append("LTRIM(RTRIM(");
                this.Visit(m.Object);
                _whereConditionBuilder.Append("))");

                return m;
            }

            #endregion

            #region StartsWith

            if (m.Method.Name == "StartsWith")
            {
                this.Visit(m.Object);
                _whereConditionBuilder.Append(" LIKE ");

                _parameter.Append = "%";
                this.Visit(m.Arguments[0]);
                return m;
            }

            #endregion

            #region EndsWith

            if (m.Method.Name == "EndsWith")
            {
                this.Visit(m.Object);
                _whereConditionBuilder.Append(" LIKE ");

                _parameter.Prepend = "%";
                this.Visit(m.Arguments[0]);
                return m;
            }

            #endregion

            #region Contains            

            if (m.Method.Name == "Contains")
            {
                if (m.Object == null)
                {
                    this.Visit((MemberExpression)m.Arguments[1]);
                    _whereConditionBuilder.Append(" IN ");
                    var memberExpression = (MemberExpression)m.Arguments[0];
                    if (memberExpression.Expression.NodeType == ExpressionType.Constant) this.Visit(memberExpression.Expression);
                }
                else
                {
                    this.Visit(m.Object);
                    _whereConditionBuilder.Append(" LIKE ");

                    _parameter.Prepend = "%";
                    _parameter.Append = "%";
                    this.Visit(m.Arguments[0]);
                }

                return m;
            }

            #endregion

            #region TrimStart

            if (m.Method.Name == "TrimStart")
            {
                _whereConditionBuilder.Append("LTRIM(");
                this.Visit(m.Object);
                _whereConditionBuilder.Append(")");

                return m;
            }

            #endregion

            #region TrimEnd

            if (m.Method.Name == "TrimEnd")
            {
                _whereConditionBuilder.Append("RTRIM(");
                this.Visit(m.Object);
                _whereConditionBuilder.Append(")");

                return m;
            }

            #endregion

            #region ToUpper

            if (m.Method.Name == "ToUpper")
            {
                _whereConditionBuilder.Append("UPPER(");
                this.Visit(m.Object);
                _whereConditionBuilder.Append(")");

                return m;
            }

            #endregion

            #region ToLower

            if (m.Method.Name == "ToLower")
            {
                _whereConditionBuilder.Append("LOWER(");
                this.Visit(m.Object);
                _whereConditionBuilder.Append(")");

                return m;
            }

            #endregion

            #region Substring

            if (m.Method.Name == "Substring")
            {
                int intResult;

                _whereConditionBuilder.Append("SUBSTRING(");
                this.Visit(m.Object);

                var startParameter = (ConstantExpression)m.Arguments[0];
                if (!int.TryParse(startParameter?.Value.ToString(), out intResult)) throw new ArgumentNullException();
                _whereConditionBuilder.Append(", " + intResult);

                var lenParameter = (ConstantExpression)m.Arguments[1];
                if (!int.TryParse(lenParameter?.Value.ToString(), out intResult)) throw new ArgumentNullException();
                _whereConditionBuilder.Append(", " + intResult + ")");

                return m;
            }

            #endregion

            #region ToString

            if (m.Method.Name == "ToString")
            {
                _whereConditionBuilder.Append("CONVERT(varchar(MAX), ");
                this.Visit(m.Object);
                _whereConditionBuilder.Append(")");

                return m;
            }

            #endregion

            #region Equals

            if (m.Method.Name == "Equals" && m.Object != null)
            {
                this.Visit(m.Object);
                _whereConditionBuilder.Append(" = ");
                this.Visit(m.Arguments[0]);

                return m;
            }

            #endregion

            throw new NotSupportedException($"The method '{m.Method.Name}' is not supported.");
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
                    throw new NotSupportedException($"The unary operator '{u.NodeType}' is not supported.");
            }

            return u;
        }

        protected override Expression VisitBinary(BinaryExpression b)
        {
            _whereConditionBuilder.Append("(");

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
                    throw new NotSupportedException($"The binary operator '{b.NodeType}' is not supported.");
            }

            this.Visit(b.Right);

            _whereConditionBuilder.Append(")");

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
                        _whereConditionBuilder.Append(this.ToSqlFormat(c.Value.GetType(), c.Value));
                        break;

                    case TypeCode.String:
                        _whereConditionBuilder.Append("'");
                        _whereConditionBuilder.Append(_parameter.Prepend);
                        _whereConditionBuilder.Append(this.ToSqlFormat(c.Value.GetType(), c.Value));
                        _whereConditionBuilder.Append(_parameter.Append);
                        _whereConditionBuilder.Append("'");
                        break;

                    case TypeCode.Decimal:
                        _whereConditionBuilder.Append(this.ToSqlFormat(c.Value.GetType(), c.Value));
                        break;

                    case TypeCode.Double:
                        _whereConditionBuilder.Append(this.ToSqlFormat(c.Value.GetType(), c.Value));
                        break;

                    case TypeCode.DateTime:
                        _whereConditionBuilder.Append("'");
                        _whereConditionBuilder.Append(this.ToSqlFormat(c.Value.GetType(), c.Value));
                        _whereConditionBuilder.Append("'");
                        break;

                    case TypeCode.Object:
                        Type previousType = null;
                        var fieldInfos = c.Type.GetFields(BindingFlags.Public | BindingFlags.Instance);
                        if (typeof(IEnumerable).IsAssignableFrom(fieldInfos[0].FieldType))
                        {
                            var valuesToPrint = new List<string>();
                            var values = (IEnumerable)fieldInfos[0].GetValue(c.Value);
                            foreach (var value in values)
                            {
                                if (!_types.Contains(value.GetType())) throw new ArgumentException();
                                if (previousType != null && previousType != value.GetType()) throw new ArgumentException();

                                var quotes = this.Quotes(value.GetType());
                                valuesToPrint.Add($"{quotes}{this.ToSqlFormat(value.GetType(), value)}{quotes}");

                                previousType = value.GetType();
                            }
                            _whereConditionBuilder.Append("(" + string.Join(",", valuesToPrint) + ")");
                        }
                        else
                        {
                            throw new NotSupportedException($"The constant for '{c.Value}' is not supported");
                        }

                        break;

                    default:
                        _whereConditionBuilder.Append(this.ToSqlFormat(c.Value.GetType(), c.Value));
                        break;
                }
            }

            return c;
        }

        protected override Expression VisitMember(MemberExpression m)
        {
            var constantExpression = m.Expression as ConstantExpression;
            if (constantExpression != null)
            {
                var lambda = Expression.Lambda(m);
                var fn = lambda.Compile();
                return this.Visit(Expression.Constant(fn.DynamicInvoke(null), m.Type));
            }

            var subMemberExpression = m.Expression as MemberExpression;
            if (subMemberExpression != null)
            {
                throw new NotSupportedException("Cannot manage complex properties");
            }

            _whereConditionBuilder.Append($"[{this.GetDataBaseColumnName(m.Member.Name)}]");

            return m;
        }

        #endregion

        #region Private Methods

        private ModelToTableMapper<T> CreateModelToTableMapperHelper()
        {
            var modelPropertyInfosWithColumnAttribute = typeof(T)
                    .GetProperties(BindingFlags.GetProperty | BindingFlags.Instance | BindingFlags.Public)
                    .Where(x => CustomAttributeExtensions.IsDefined(x, typeof(ColumnAttribute), false))
                    .ToArray();

            if (!modelPropertyInfosWithColumnAttribute.Any()) return null;

            var mapper = new ModelToTableMapper<T>();
            foreach (var propertyInfo in modelPropertyInfosWithColumnAttribute)
            {
                var attribute = propertyInfo.GetCustomAttribute(typeof(ColumnAttribute));
                var dbColumnName = ((ColumnAttribute)attribute)?.Name;
                if (string.IsNullOrWhiteSpace(dbColumnName))
                {
                    dbColumnName = propertyInfo.Name;
                    mapper.AddMapping(propertyInfo, dbColumnName);
                }

                mapper.AddMapping(propertyInfo, dbColumnName);
            }

            return mapper;
        }

        private string GetDataBaseColumnName(string memberName)
        {
            if (_modelMapperDictionary == null || !_modelMapperDictionary.Any()) return memberName;
            var mapping = _modelMapperDictionary.FirstOrDefault(mm => mm.Key.ToLower() == memberName.ToLower());

            return default(KeyValuePair<string, string>).Equals(mapping)
                ? memberName
                : mapping.Value.Replace("[", string.Empty).Replace("]", string.Empty);
        }

        private string ToSqlFormat(Type type, object value)
        {
            if (type == typeof(bool)) return (bool)value ? "1" : "0";

            if (type == typeof(string)) return value.ToString();

            if (type == typeof(decimal)) return Convert.ToDecimal(value).ToString("g", CultureInfo.InvariantCulture);

            if (type == typeof(double)) return Convert.ToDouble(value).ToString("g", CultureInfo.InvariantCulture);

            if (type == typeof(DateTime)) return Convert.ToDateTime(value).ToString("s", CultureInfo.InvariantCulture);

            return value.ToString();
        }

        protected string Quotes(Type type)
        {
            if (type == typeof(string)) return "'";
            if (type == typeof(DateTime)) return "'";

            return string.Empty;
        }

        protected bool IsNullConstant(Expression exp)
        {
            return exp.NodeType == ExpressionType.Constant && ((ConstantExpression)exp).Value == null;
        }

        #endregion
    }
}