using System;
using System.Collections.Generic;
using System.Reflection;

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