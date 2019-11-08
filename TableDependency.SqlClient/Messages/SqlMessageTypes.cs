#region License
// TableDependency, SqlTableDependency
// Copyright (c) 2015-2020 Christian Del Bianco. All rights reserved.
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

namespace TableDependency.SqlClient.Messages
{
    public static class SqlMessageTypes
    {
        /// <value>
        /// System message type for event notification messages.
        /// </value>
        public const string EventNotificationType = "http://schemas.microsoft.com/SQL/Notifications/EventNotification";

        /// <value>
        /// System message type for query notification messages.
        /// </value>
        public const string QueryNotificationType = "http://schemas.microsoft.com/SQL/Notifications/QueryNotification";

        /// <value>
        /// System message type for message indicating failed remote service binding.
        /// </value>
        public const string FailedRemoteServiceBindingType = "http://schemas.microsoft.com/SQL/ServiceBroker/BrokerConfigurationNotice/FailedRemoteServiceBinding";

        /// <value>
        /// System message type for message indicating failed route.
        /// </value>
        public const string FailedRouteType = "http://schemas.microsoft.com/SQL/ServiceBroker/BrokerConfigurationNotice/FailedRoute";

        /// <value>
        /// System message type for message indicating missing remote service binding.
        /// </value>
        public const string MissingRemoteServiceBindingType = "http://schemas.microsoft.com/SQL/ServiceBroker/BrokerConfigurationNotice/MissingRemoteServiceBinding";

        /// <value>
        /// System message type for message indicating missing route.
        /// </value>
        public const string MissingRouteType = "http://schemas.microsoft.com/SQL/ServiceBroker/BrokerConfigurationNotice/MissingRoute";

        /// <value>
        /// System message type for dialog timer messages.
        /// </value>
        public const string DialogTimerType = "http://schemas.microsoft.com/SQL/ServiceBroker/DialogTimer";

        /// <value>
        /// System message type for message indicating end of dialog.
        /// </value>
        public const string EndDialogType = "http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog";

        /// <value>
        /// System message type for error messages.
        /// </value>
        public const string ErrorType = "http://schemas.microsoft.com/SQL/ServiceBroker/Error";

        /// <value>
        /// System message type for diagnostic description messages.
        /// </value>
        public const string DescriptionType = "http://schemas.microsoft.com/SQL/ServiceBroker/ServiceDiagnostic/Description";

        /// <value>
        /// System message type for diagnostic query messages.
        /// </value>
        public const string QueryType = "http://schemas.microsoft.com/SQL/ServiceBroker/ServiceDiagnostic/Query";

        /// <value>
        /// System message type for diagnostic status messages.
        /// </value>
        public const string StatusType = "http://schemas.microsoft.com/SQL/ServiceBroker/ServiceDiagnostic/Status";

        /// <value>
        /// System message type for echo service messages.
        /// </value>
        public const string EchoType = "http://schemas.microsoft.com/SQL/ServiceBroker/ServiceEcho/Echo";
    }
}