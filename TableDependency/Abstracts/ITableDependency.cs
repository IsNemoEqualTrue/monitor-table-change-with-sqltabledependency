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
using System.Diagnostics;
using System.Globalization;
using System.Text;

using TableDependency.Delegates;
using TableDependency.Enums;

namespace TableDependency.Abstracts
{
    public interface ITableDependency : IDisposable
    {
        #region Events

        event ErrorEventHandler OnError;
        event StatusEventHandler OnStatusChanged;

        #endregion

        #region Methods

        void Start(int timeOut = 120, int watchDogTimeOut = 180);
        void Stop();

        #endregion

        #region Properties

        TraceLevel TraceLevel { get; set; }
        TraceListener TraceListener { get; set; }
        TableDependencyStatus Status { get; }
        Encoding Encoding { get; set; }
        CultureInfo CultureInfo { get; set; }
        string DataBaseObjectsNamingConvention { get; }
        string TableName { get; }
        string SchemaName { get; }

        #endregion
    }

    public interface ITableDependency<T> : ITableDependency where T : class, new()
    {
        #region Events

        event ChangedEventHandler<T> OnChanged;

        #endregion
    }
}