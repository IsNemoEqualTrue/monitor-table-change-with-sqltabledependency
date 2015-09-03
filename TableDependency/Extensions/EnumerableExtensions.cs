using System;
using System.Collections.Generic;

namespace TableDependency.Extensions
{
    internal static class EnumerableExtensions
    {
        internal static void ForEach<T>(this IEnumerable<T> enumeration, Action<T> action)
        {
            foreach (var item in enumeration)
            {
                action(item);
            }
        }
    }
}