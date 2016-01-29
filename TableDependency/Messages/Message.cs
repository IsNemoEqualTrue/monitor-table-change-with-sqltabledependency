////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   © 2015-2106 Christian Del Bianco. All rights reserved.
////////////////////////////////////////////////////////////////////////////////
namespace TableDependency.Messages
{
    internal class Message
    {
        internal string Recipient { get; }
        internal byte[] Body { get; }

        #region Constructors

        internal Message(string recipient, byte[] body)
        {
            this.Recipient = recipient;
            this.Body = body;
        }

        #endregion
    }
}