#region License
// TableDependency, SqlTableDependency
// Copyright (c) 2015-2019 Christian Del Bianco. All rights reserved.
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

using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;

using TableDependency.SqlClient.Base.Abstracts;

namespace TableDependency.SqlClient.Base.Utilities
{
    public static class ModelToTableMapperHelper<T> where T : class, new()
    {
        public static IModelToTableMapper<T> GetModelMapperFromColumnDataAnnotation(IEnumerable<TableColumnInfo> tableColumns)
        {
            var modelPropertyInfosWithColumnAttribute = typeof(T)
                .GetProperties(BindingFlags.GetProperty | BindingFlags.Instance | BindingFlags.Public)
                .Where(x => CustomAttributeExtensions.IsDefined((MemberInfo)x, typeof(ColumnAttribute), false))
                .ToArray();

            if (!modelPropertyInfosWithColumnAttribute.Any()) return null;

            var mapper = new ModelToTableMapper<T>();
            foreach (var propertyInfo in modelPropertyInfosWithColumnAttribute)
            {
                var attribute = propertyInfo.GetCustomAttribute(typeof(ColumnAttribute));
                var dbColumnName = ((ColumnAttribute)attribute)?.Name;
                if (string.IsNullOrWhiteSpace(dbColumnName) && tableColumns.Any(tc => tc.Name == propertyInfo.Name))
                {
                    dbColumnName = propertyInfo.Name;
                    mapper.AddMapping(propertyInfo, dbColumnName);
                    continue;
                }

                if (!string.IsNullOrWhiteSpace(dbColumnName))
                {
                    mapper.AddMapping(propertyInfo, dbColumnName);
                }
            }

            return mapper;
        }
    }
}