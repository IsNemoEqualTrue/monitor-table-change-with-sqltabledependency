#region License
// TableDependency, SqlTableDependency - Release 8.5.2
// Copyright (c) 2015-2019 Christian Del Bianco. All rights reserved.
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

using System.ComponentModel;

namespace TableDependency.SqlClient.Enumerations
{
    /// <summary>
    /// https://docs.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-conversation-endpoints-transact-sql?view=sql-server-2017
    /// </summary>
    public enum ConversationEndpointState
    {
        [Description("STARTED_OUTBOUND")]
        SO,

        [Description("STARTED_INBOUND")]
        SI,

        [Description("CONVERSING")]
        CO,

        [Description("DISCONNECTED_INBOUND")]
        DI,

        [Description("DISCONNECTED_OUTBOUND")]
        DO,

        [Description("ERROR")]
        ER,

        [Description("CLOSED")]
        CD
    }
}