#region License
// TableDependency, SqlTableDependency, OracleTableDependency
// Copyright (c) 2015-2106 Christian Del Bianco. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation
// files (the "Software"), to deal in the Software without
// restriction, including without limitation the rights to use,
// copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
#endregion
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TableDependency.Enums;
using TableDependency.Exceptions;

namespace TableDependency.Messages
{
    internal class MessagesBag
    {
        #region Member variables

        private readonly string _endMessageSignature;
        private readonly string _startMessageSignature;

        #endregion

        #region Properties
        public Encoding Encoding { get; }
        public List<Message> MessageSheets { get; }
        public ChangeType MessageType { get; private set; }
        public MessagesBagStatus Status { get; private set; }

        #endregion

        #region Constructors

        internal MessagesBag(Encoding encoding, string startMessageSignature, string endMessageSignature)
        {            
            this.MessageSheets = new List<Message>();
            this.Status = MessagesBagStatus.Open;

            this.Encoding = encoding;
            this._endMessageSignature = endMessageSignature;
            this._startMessageSignature = startMessageSignature;
        }

        #endregion

        #region Public Methods

        internal MessagesBagStatus AddMessage(string rawMessageType, byte[] messageValue)
        {
            if (rawMessageType == this._startMessageSignature)
            {
                this.MessageType = (ChangeType)Enum.Parse(typeof(ChangeType), this.Encoding.GetString(messageValue));
                this.MessageSheets.Clear();
                return (this.Status = MessagesBagStatus.Open);
            }

            if (rawMessageType == this._endMessageSignature)
            {
                if ((ChangeType)Enum.Parse(typeof(ChangeType), this.Encoding.GetString(messageValue)) != this.MessageType) throw new DataMisalignedException();
                return (this.Status = MessagesBagStatus.Closed);
            }

            if (this.Status == MessagesBagStatus.Closed) throw new MessageMisalignedException("Envelop already closed!");
            if (this.MessageType != GetMessageType(rawMessageType)) throw new MessageMisalignedException();

            this.MessageSheets.Add(new Message(GetRecipient(rawMessageType), messageValue));

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