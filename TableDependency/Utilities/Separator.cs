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

using static System.String;

namespace TableDependency.Utilities
{
    public class Separator
    {
        /// <summary>
        /// The current index.
        /// </summary>
        private int _currentIndex = 1;

        /// <summary>
        /// The index from which to start providing the separator.
        /// </summary>
        private int _startIndex;

        /// <summary>
        /// The separator string.
        /// </summary>
        private string _separator;

        /// <summary>
        /// Initializes a new instance of the <see cref="Separator"/> class.
        /// </summary>
        /// <param name="separator">The separator.</param>
        public Separator(string separator) : this(1, separator)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Separator"/> class.
        /// </summary>
        /// <param name="startIndex">The start index.</param>
        /// <param name="separator">The separator.</param>
        public Separator(int startIndex, string separator)
        {
            _separator = separator;
            _startIndex = startIndex;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Separator"/> class.
        /// </summary>
        /// <param name="startIndex">The start index.</param>
        /// <param name="separator">The separator.</param>
        public Separator(int startIndex, char separator)
        {
            _separator = Concat(Empty, separator);
            _startIndex = startIndex;
        }

        /// <summary>
        /// Gets the separator.
        /// </summary>
        /// <returns>The separator string.</returns>
        public string GetSeparator()
        {
            return _currentIndex++ >= _startIndex ? _separator : Empty;
        }

        /// <summary>
        /// Resets the index.
        /// </summary>
        public void Reset()
        {
            _currentIndex = 1;
        }

        /// <summary>
        /// Resets the separator.
        /// </summary>
        /// <param name="startIndex">The start index.</param>
        /// <param name="separator">The separator.</param>
        public void Reset(int startIndex, string separator)
        {
            _currentIndex = 1;
            _separator = separator;
            _startIndex = startIndex;
        }

        /// <summary>
        /// Resets the separator.
        /// </summary>
        /// <param name="startIndex">The start index.</param>
        public void Reset(int startIndex)
        {
            _currentIndex = 1;
            _startIndex = startIndex;
        }
    }
}