#region License
// TableDependency, SqlTableDependency, OracleTableDependency
// Copyright (c) 2015-2017 Christian Del Bianco. All rights reserved.
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
    public class MessagesBag
    {
        #region Member variables

        private readonly string _endMessageSignature;
        private readonly IList<string> _startMessagesSignature;

        #endregion

        #region Properties
        public Encoding Encoding { get; }
        public ChangeType MessageType { get; private set; }
        public List<Message> MessageSheets { get; }        
        public MessagesBagStatus Status { get; private set; }

        #endregion

        #region Constructors

        public MessagesBag(Encoding encoding, IList<string> startMessagesSignature, string endMessageSignature)
        {            
            this.MessageSheets = new List<Message>();
            this.Status = MessagesBagStatus.None;
            this.Encoding = encoding;

            _endMessageSignature = endMessageSignature;
            _startMessagesSignature = startMessagesSignature;
        }

        #endregion

        #region Public Methods

        public MessagesBagStatus AddMessage(string rawMessageType, byte[] messageValue)
        {
            if (_startMessagesSignature.Contains(rawMessageType))
            {
                if (this.Status != MessagesBagStatus.None) throw new MessageMisalignedException($"Received an StartMessege while current status is {this.Status}.");

                this.MessageType = GetMessageType(rawMessageType);
                this.MessageSheets.Clear();

                return (this.Status = MessagesBagStatus.Open);
            }

            if (rawMessageType == _endMessageSignature)
            {
                if (this.Status != MessagesBagStatus.Collecting) throw new MessageMisalignedException($"Received an EndMessege while current status is {this.Status}.");
                return (this.Status = MessagesBagStatus.Closed);
            }

            if (this.Status == MessagesBagStatus.Closed)
            {
                throw new MessageMisalignedException($"Received {rawMessageType} while current status is {MessagesBagStatus.Closed}.");
            }

            this.MessageSheets.Add(new Message(GetRecipient(rawMessageType), messageValue));
            return (this.Status = MessagesBagStatus.Collecting);
        }

        #endregion

        #region Private Methods

        private static ChangeType GetMessageType(string rawMessageType)
        {
            var messageChunk = rawMessageType.Split('/');

            if (string.Compare(messageChunk[2], ChangeType.Delete.ToString(), StringComparison.OrdinalIgnoreCase) == 0)
                return ChangeType.Delete;
            if (string.Compare(messageChunk[2], ChangeType.Insert.ToString(), StringComparison.OrdinalIgnoreCase) == 0)
                return ChangeType.Insert;
            if (string.Compare(messageChunk[2], ChangeType.Update.ToString(), StringComparison.OrdinalIgnoreCase) == 0)
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