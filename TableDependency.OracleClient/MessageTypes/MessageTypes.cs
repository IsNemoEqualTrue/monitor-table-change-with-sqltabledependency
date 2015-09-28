////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
namespace TableDependency.OracleClient.MessageTypes
{
    public static class CustomMessageTypes
    {
        internal const string DeletedMessageType = "Delete";

        internal const string InsertedMessageType = "Insert";

        internal const string UpdatedMessageType = "Update";

        internal const string TimeoutMessageType = "Timeout";
    }
}