////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TableDependency.Enums;
using TableDependency.Exceptions;

namespace TableDependency.SqlClient.Messages
{
    internal class MessagesBag
    {
        #region Member variables

        private readonly string _endMessageSignature;
        private readonly string _startMessageSignature;

        #endregion

        #region Properties

        public List<Message> MessageSheets { get; }
        public ChangeType MessageType { get; private set; }
        public MessagesBagStatus Status { get; private set; }

        #endregion

        #region Constructors

        public MessagesBag(string startMessageSignature, string endMessageSignature)
        {
            MessageSheets = new List<Message>();
            Status = MessagesBagStatus.Open;

            _endMessageSignature = endMessageSignature;
            _startMessageSignature = startMessageSignature;
        }

        #endregion

        #region Public Methods

        public MessagesBagStatus AddMessage(string rawMessageType, byte[] messageValue)
        {
            if (rawMessageType == _startMessageSignature)
            {               
                MessageType = (ChangeType)Enum.Parse(typeof(ChangeType), Encoding.Unicode.GetString(messageValue));
                MessageSheets.Clear();
                return (Status = MessagesBagStatus.Open);
            }

            if (rawMessageType == _endMessageSignature)
            {
                if ((ChangeType)Enum.Parse(typeof(ChangeType), Encoding.Unicode.GetString(messageValue)) != MessageType) throw new DataMisalignedException();
                return (Status = MessagesBagStatus.Closed);
            }

            if (Status == MessagesBagStatus.Closed) throw new MessageMisalignedException("Envelop already closed!");
            if (MessageType != GetMessageType(rawMessageType)) throw new MessageMisalignedException();

            MessageSheets.Add(new Message(GetRecipient(rawMessageType), messageValue));

            return MessagesBagStatus.Collecting;
        }

        #endregion

        #region Private Methods

        private static ChangeType GetMessageType(string rawMessageType)
        {
            var messageChunk = rawMessageType.Split('/');

            if (messageChunk[1] == ChangeType.Delete.ToString())
                return ChangeType.Delete;
            if (messageChunk[1] == ChangeType.Insert.ToString())
                return ChangeType.Insert;
            if (messageChunk[1] == ChangeType.Update.ToString())
                return ChangeType.Update;

            return ChangeType.None;
        }

        private static string GetRecipient(string rawMessageType)
        {
            return rawMessageType.Split('/').Last();
        }

        #endregion
    }
}