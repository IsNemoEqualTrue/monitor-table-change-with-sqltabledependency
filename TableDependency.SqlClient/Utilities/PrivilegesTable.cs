#region License
// TableDependency, SqlTableDependency
// Copyright (c) 2015-2020 Christian Del Bianco. All rights reserved.
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
using System.Collections.Generic;

namespace TableDependency.SqlClient.Utilities
{
    internal class PrivilegesTable
    {
        public List<Privilege> Rows { get; set; } = new List<Privilege>();

        public static PrivilegesTable FromEnumerable(IEnumerable<Dictionary<string, object>> rows)
        {
            var privilegesTable = new PrivilegesTable();
            foreach (var row in rows) privilegesTable.Rows.Add(Privilege.FromDictionary(row));
            return privilegesTable;
        }
    }

    internal class Privilege
    {
        public string UserName { get; set; }
        public string UserType { get; set; }
        public string DatabaseUserName { get; set; }
        public string Role { get; set; }
        public string PermissionType { get; set; }
        public string PermissionState { get; set; }
        public string ObjectType { get; set; }
        public string ObjectName { get; set; }
        public string ColumnName { get; set; }

        public static Privilege FromDictionary(Dictionary<string, object> columns)
        {
            var privilege = new Privilege();

            foreach (var column in columns)
            {
                foreach (var propertyInfo in privilege.GetType().GetProperties())
                {
                    if (column.Key != propertyInfo.Name) continue;

                    var theValue = column.Value == DBNull.Value ? null : column.Value;
                    propertyInfo.SetValue(privilege, theValue);
                    break;
                }
            }

            return privilege;
        }
    }    
}