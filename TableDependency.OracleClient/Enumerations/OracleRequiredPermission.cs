////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   © 2015-2106 Christian Del Bianco. All rights reserved.
////////////////////////////////////////////////////////////////////////////////

using System.ComponentModel;

namespace TableDependency.OracleClient.Enumerations
{
    public enum OracleRequiredPermission
    {
        [Description("CREATE TYPE")]
        CreateType,
        [Description("CREATE TYPE")]
        DropType,

        [Description("CREATE TRIGGER")]
        CreateTrigger,
        [Description("DROP TRIGGER")]
        DropTrigger,

        [Description("CREATE PROCEDURE")]
        CreateProcedure,
        [Description("DROP PROCEDURE")]
        DropProcedure
    }
}