////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   © 2015-2106 Christian Del Bianco. All rights reserved.
////////////////////////////////////////////////////////////////////////////////

using System.ComponentModel;

namespace TableDependency.SqlClient.Enumerations
{
    internal enum SqlServerSelectGrantOnSysView
    {
        [Description("TRIGGERS")]
        SysTriggers,
        [Description("PROCEDURES")]
        SysProcedure,
        [Description("SERVICES")]
        SysServices,
        [Description("SERVICE_QUEUES")]
        SysServiceQueues,
        [Description("SERVICE_CONTRACTS")]
        SysServiceContracts,
        [Description("SERVICE_MESSAGE_TYPES")]
        SysServiceMessageTypes
    }
}