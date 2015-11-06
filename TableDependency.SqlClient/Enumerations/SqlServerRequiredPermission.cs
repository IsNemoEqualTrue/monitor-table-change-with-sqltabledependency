////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////

namespace TableDependency.SqlClient.Enumerations
{
    internal enum SqlServerRequiredPermission
    {
        CreateType,
        DropType,
        CreateTrigger,
        CreateProcedure,
        DropProcedure,
        CreateJob,
        DropJob
    }
}