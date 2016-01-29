////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   © 2015-2106 Christian Del Bianco. All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;
using System.Collections.Generic;
using System.Linq;

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

        public static IEnumerable<T> ForEach<T>(this IEnumerable<T> array, Action<T, int> act)
        {
            var i = 0;
            var forEach = array as T[] ?? array.ToArray();
            foreach (var arr in forEach) act(arr, i++);
            return forEach;
        }

        public static IEnumerable<TRt> ForEach<T, TRt>(this IEnumerable<T> array, Func<T, TRt> func)
        {
            return array.Select(func).Where(obj => obj != null).ToList();
        }
    }
}