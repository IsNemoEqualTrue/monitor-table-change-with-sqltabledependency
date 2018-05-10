#region License
// TableDependency, SqlTableDependency
// Copyright (c) 2015-2018 Christian Del Bianco. All rights reserved.
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
using System.Text;
using TableDependency.Enums;
using TableDependency.Exceptions;

namespace TableDependency.Messages
{
    public class MessagesBag
    {
        #region Member variables

        private readonly string _endMessageSignature;
        private readonly ICollection<string> _processableMessages;
        private readonly IList<string> _startMessagesSignature;

        #endregion

        #region Properties

        public Encoding Encoding { get; }
        public ChangeType MessageType { get; private set; }
        public List<Message> Messages { get; private set; }
        public MessagesBagStatus Status { get; private set; }
        public bool Ready => this.Status == MessagesBagStatus.Collecting;
        
        #endregion

        #region Constructors

        public MessagesBag(Encoding encoding, IList<string> startMessagesSignature, string endMessageSignature, ICollection<string> processableMessages)
        {            
            this.Messages = new List<Message>();
            this.Status = MessagesBagStatus.Empty;
            this.Encoding = encoding;

            _endMessageSignature = endMessageSignature;
            _startMessagesSignature = startMessagesSignature;
            _processableMessages = processableMessages;
        }

        #endregion

        #region Public Methods

        public void Reset()
        {            
            this.Status = MessagesBagStatus.Empty;
        }

        public MessagesBagStatus AddMessage(Message message)
        {
            if (_startMessagesSignature.Contains(message.MessageType))
            {
                if (this.Status != MessagesBagStatus.Empty) throw new MessageMisalignedException($"Received an StartMessege while current status is {this.Status}.");
                this.MessageType = MessagesBag.GetMessageType(message.MessageType);
                this.Messages.Clear();
                return this.Status = MessagesBagStatus.Collecting;
            }

            if (message.MessageType == _endMessageSignature)
            {
                if (this.Status != MessagesBagStatus.Collecting) throw new MessageMisalignedException($"Received an EndMessege while current status is {this.Status}.");
                return this.Status = MessagesBagStatus.Ready;
            }

            if (this.Status == MessagesBagStatus.Ready)
            {
                throw new MessageMisalignedException($"Received {message.MessageType} message while current status is {MessagesBagStatus.Ready}.");
            }

            if (_processableMessages.Contains(message.MessageType) == false)
            {
                throw new MessageMisalignedException($"Queue containing a message type not expected [{message.MessageType}].");
            }

            this.Messages.Add(message);

            return this.Status = MessagesBagStatus.Collecting;
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

        #endregion
    }
}