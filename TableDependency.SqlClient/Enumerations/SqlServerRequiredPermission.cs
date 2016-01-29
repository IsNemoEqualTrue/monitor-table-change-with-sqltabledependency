////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   © 2015-2106 Christian Del Bianco. All rights reserved.
////////////////////////////////////////////////////////////////////////////////

using System.ComponentModel;

namespace TableDependency.SqlClient.Enumerations
{
    /// <summary>
    /// https://msdn.microsoft.com/en-us/library/ms178569.aspx
    /// </summary>
    internal enum SqlServerRequiredPermission
    {
        [Description("CREATE MESSAGE TYPE")]
        CreateMessageType,
        [Description("CREATE CONTRACT")]
        CreateContract,
        [Description("CREATE QUEUE")]
        CreateQueue,
        [Description("CREATE SERVICE")]
        CreateService,
        [Description("CREATE PROCEDURE")]
        CreateProcedure
    }
}