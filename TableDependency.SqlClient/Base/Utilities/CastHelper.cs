using System;
using System.Collections.Generic;

namespace TableDependency.SqlClient.Base.Utilities
{
    public static class CastHelper
    {
        public static readonly Dictionary<Type, object> MaxValues = new Dictionary<Type, object>()
        {
            { typeof(bool), 1},
            { typeof(char), char.MaxValue},
            { typeof(sbyte), sbyte.MaxValue},
            { typeof(byte), byte.MaxValue},
            { typeof(short), short.MaxValue},
            { typeof(ushort), ushort.MaxValue},
            { typeof(int), int.MaxValue},
            { typeof(uint), uint.MaxValue},
            { typeof(long), long.MaxValue},
            { typeof(ulong), ulong.MaxValue},
            { typeof(decimal), decimal.MaxValue},
            { typeof(double), double.MaxValue},
            { typeof(float), float.MaxValue}
        };


        public static bool IsSafeToConvert(byte valueToConvert, string typeToConvertTo)
        {
            return (IsSafeToConvert((ulong)valueToConvert, typeToConvertTo));
        }

        public static bool IsSafeToConvert(sbyte valueToConvert, string typeToConvertTo)
        {
            return (IsSafeToConvert((long)valueToConvert, typeToConvertTo));
        }


        public static bool IsSafeToConvert(short valueToConvert, string typeToConvertTo)
        {
            return (IsSafeToConvert((long)valueToConvert, typeToConvertTo));
        }

        public static bool IsSafeToConvert(ushort valueToConvert, string typeToConvertTo)
        {
            return (IsSafeToConvert((ulong)valueToConvert, typeToConvertTo));
        }


        public static bool IsSafeToConvert(int valueToConvert, string typeToConvertTo)
        {
            return (IsSafeToConvert((long)valueToConvert, typeToConvertTo));
        }

        public static bool IsSafeToConvert(uint valueToConvert, string typeToConvertTo)
        {
            return (IsSafeToConvert((ulong)valueToConvert, typeToConvertTo));
        }


        public static bool IsSafeToConvert(ulong valueToConvert, string typeToConvertTo)
        {
            bool isSafe = false;

            switch (typeToConvertTo)
            {
                case "byte":
                    if (valueToConvert <= byte.MaxValue && valueToConvert >= 0)
                        isSafe = true;
                    break;

                case "sbyte":
                    if (valueToConvert <= (ulong)sbyte.MaxValue &&
                       valueToConvert >= 0)
                        isSafe = true;
                    break;

                case "short":
                    if (valueToConvert <= (ulong)short.MaxValue &&
                       valueToConvert >= 0)
                        isSafe = true;
                    break;

                case "ushort":
                    if (valueToConvert <= ushort.MaxValue && valueToConvert >= 0)
                        isSafe = true;
                    break;

                case "int":
                    if (valueToConvert <= int.MaxValue && valueToConvert >= 0)
                        isSafe = true;
                    break;

                case "uint":
                    if (valueToConvert <= uint.MaxValue && valueToConvert >= 0)
                        isSafe = true;
                    break;

                case "long":
                    if (valueToConvert <= long.MaxValue && valueToConvert >= 0)
                        isSafe = true;
                    break;

                case "ulong":
                    if (valueToConvert <= ulong.MaxValue && valueToConvert >= 0)
                        isSafe = true;
                    break;

                case "char":
                    if (valueToConvert <= char.MaxValue && valueToConvert >= 0)
                        isSafe = true;
                    break;

                default:
                    isSafe = true;
                    break;
            }

            return (isSafe);
        }

        public static bool IsSafeToConvert(long valueToConvert, string typeToConvertTo)
        {
            bool isSafe = false;

            switch (typeToConvertTo)
            {
                case "byte":
                    if (valueToConvert <= byte.MaxValue &&
                       valueToConvert >= byte.MinValue)
                        isSafe = true;
                    break;

                case "sbyte":
                    if (valueToConvert <= sbyte.MaxValue &&
                       valueToConvert >= sbyte.MinValue)
                        isSafe = true;
                    break;

                case "short":
                    if (valueToConvert <= short.MaxValue &&
                       valueToConvert >= short.MinValue)
                        isSafe = true;
                    break;

                case "ushort":
                    if (valueToConvert <= ushort.MaxValue &&
                       valueToConvert >= ushort.MinValue)
                        isSafe = true;
                    break;

                case "int":
                    if (valueToConvert <= int.MaxValue &&
                       valueToConvert >= int.MinValue)
                        isSafe = true;
                    break;

                case "uint":
                    if (valueToConvert <= uint.MaxValue &&
                       valueToConvert >= uint.MinValue)
                        isSafe = true;
                    break;

                case "long":
                    if (valueToConvert <= long.MaxValue &&
                       valueToConvert >= long.MinValue)
                        isSafe = true;
                    break;

                case "ulong":
                    if (valueToConvert >= 0)
                        isSafe = true;
                    break;

                case "char":
                    if (valueToConvert <= char.MaxValue &&
                       valueToConvert >= char.MinValue)
                        isSafe = true;
                    break;

                default:
                    isSafe = true;
                    break;
            }

            return (isSafe);
        }


        public static bool IsSafeToConvert(char valueToConvert, string typeToConvertTo)
        {
            return (IsSafeToConvert((long)valueToConvert, typeToConvertTo));
        }

        public static bool IsSafeToConvert(float valueToConvert, string typeToConvertTo)
        {
            bool isSafe = false;

            switch (typeToConvertTo)
            {
                case "byte":
                    if (valueToConvert <= byte.MaxValue &&
                       valueToConvert >= byte.MinValue)
                        isSafe = true;
                    break;

                case "sbyte":
                    if (valueToConvert <= sbyte.MaxValue &&
                       valueToConvert >= sbyte.MinValue)
                        isSafe = true;
                    break;

                case "short":
                    if (valueToConvert <= short.MaxValue &&
                       valueToConvert >= short.MinValue)
                        isSafe = true;
                    break;

                case "ushort":
                    if (valueToConvert <= ushort.MaxValue &&
                       valueToConvert >= ushort.MinValue)
                        isSafe = true;
                    break;

                case "int":
                    if (valueToConvert <= int.MaxValue &&
                       valueToConvert >= int.MinValue)
                        isSafe = true;
                    break;

                case "uint":
                    if (valueToConvert <= uint.MaxValue &&
                       valueToConvert >= uint.MinValue)
                        isSafe = true;
                    break;

                case "long":
                    if (valueToConvert <= long.MaxValue &&
                       valueToConvert >= long.MinValue)
                        isSafe = true;
                    break;

                case "ulong":
                    if (valueToConvert <= ulong.MaxValue &&
                       valueToConvert >= ulong.MinValue)
                        isSafe = true;
                    break;

                case "char":
                    if (valueToConvert <= char.MaxValue &&
                       valueToConvert >= char.MinValue)
                        isSafe = true;
                    break;

                case "double":
                    if (valueToConvert <= double.MaxValue &&
                       valueToConvert >= double.MinValue)
                        isSafe = true;
                    break;

                case "decimal":
                    if (valueToConvert <= (float)decimal.MaxValue &&
                       valueToConvert >= (float)decimal.MinValue)
                        isSafe = true;
                    break;

                default:
                    isSafe = true;
                    break;
            }

            return (isSafe);
        }


        public static bool IsSafeToConvert(double valueToConvert, string typeToConvertTo)
        {
            bool isSafe = false;

            switch (typeToConvertTo)
            {
                case "byte":
                    if (valueToConvert <= byte.MaxValue &&
                       valueToConvert >= byte.MinValue)
                        isSafe = true;
                    break;

                case "sbyte":
                    if (valueToConvert <= sbyte.MaxValue &&
                       valueToConvert >= sbyte.MinValue)
                        isSafe = true;
                    break;

                case "short":
                    if (valueToConvert <= short.MaxValue &&
                       valueToConvert >= short.MinValue)
                        isSafe = true;
                    break;

                case "ushort":
                    if (valueToConvert <= ushort.MaxValue &&
                       valueToConvert >= ushort.MinValue)
                        isSafe = true;
                    break;

                case "int":
                    if (valueToConvert <= int.MaxValue &&
                       valueToConvert >= int.MinValue)
                        isSafe = true;
                    break;

                case "uint":
                    if (valueToConvert <= uint.MaxValue &&
                       valueToConvert >= uint.MinValue)
                        isSafe = true;
                    break;

                case "long":
                    if (valueToConvert <= long.MaxValue &&
                       valueToConvert >= long.MinValue)
                        isSafe = true;
                    break;

                case "ulong":
                    if (valueToConvert <= ulong.MaxValue &&
                       valueToConvert >= ulong.MinValue)
                        isSafe = true;
                    break;

                case "char":
                    if (valueToConvert <= char.MaxValue &&
                       valueToConvert >= char.MinValue)
                        isSafe = true;
                    break;

                case "float":
                    if (valueToConvert <= float.MaxValue &&
                       valueToConvert >= float.MinValue)
                        isSafe = true;
                    break;

                case "decimal":
                    if (valueToConvert <= (double)decimal.MaxValue &&
                       valueToConvert >= (double)decimal.MinValue)
                        isSafe = true;
                    break;

                default:
                    isSafe = true;
                    break;
            }

            return (isSafe);
        }


        public static bool IsSafeToConvert(decimal valueToConvert, string typeToConvertTo)
        {
            bool isSafe = false;

            switch (typeToConvertTo)
            {
                case "byte":
                    if (valueToConvert <= byte.MaxValue &&
                       valueToConvert >= byte.MinValue)
                        isSafe = true;
                    break;

                case "sbyte":
                    if (valueToConvert <= sbyte.MaxValue &&
                       valueToConvert >= sbyte.MinValue)
                        isSafe = true;
                    break;

                case "short":
                    if (valueToConvert <= short.MaxValue &&
                       valueToConvert >= short.MinValue)
                        isSafe = true;
                    break;

                case "ushort":
                    if (valueToConvert <= ushort.MaxValue &&
                       valueToConvert >= ushort.MinValue)
                        isSafe = true;
                    break;

                case "int":
                    if (valueToConvert <= int.MaxValue &&
                       valueToConvert >= int.MinValue)
                        isSafe = true;
                    break;

                case "uint":
                    if (valueToConvert <= uint.MaxValue &&
                       valueToConvert >= uint.MinValue)
                        isSafe = true;
                    break;

                case "long":
                    if (valueToConvert <= long.MaxValue &&
                       valueToConvert >= long.MinValue)
                        isSafe = true;
                    break;

                case "ulong":
                    if (valueToConvert <= ulong.MaxValue &&
                       valueToConvert >= ulong.MinValue)
                        isSafe = true;
                    break;

                case "char":
                    if (valueToConvert <= char.MaxValue &&
                       valueToConvert >= char.MinValue)
                        isSafe = true;
                    break;

                default:
                    isSafe = true;
                    break;
            }

            return (isSafe);
        }
    }
}