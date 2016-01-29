////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   © 2015-2106 Christian Del Bianco. All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;

namespace TableDependency.Enums
{
    [Flags]
    public enum DmlTriggerType
    {        
        Delete = 1,
        Insert = 2,
        Update = 4,
        All = 8,
    }
}