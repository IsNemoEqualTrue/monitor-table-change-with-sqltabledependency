////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
namespace TableDependency.SqlClient.Messages
{
    internal class Message
    {
        public string Recipient { get; }
        public byte[] Body { get; }

        #region Constructors

        public Message(string recipient, byte[] body)
        {
            Recipient = recipient;
            Body = body;
        }

        #endregion
    }
}