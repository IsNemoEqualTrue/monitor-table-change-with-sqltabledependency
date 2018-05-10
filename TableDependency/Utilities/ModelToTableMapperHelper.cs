using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;

using TableDependency.Abstracts;
using TableDependency.Exceptions;

namespace TableDependency.Utilities
{
    public static class ModelToTableMapperHelper<T> where T : class, new()
    {
        public static IModelToTableMapper<T> GetModelMapperFromColumnDataAnnotation()
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
                if (dbColumnName != null && mapper.GetMapping(dbColumnName) != null)
                {
                    throw new ModelToTableMapperException("Duplicate mapping for column " + dbColumnName);
                }

                mapper.AddMapping(propertyInfo, dbColumnName);
            }

            return mapper;
        }
    }
}