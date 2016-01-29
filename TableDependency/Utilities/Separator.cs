////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   © 2015-2106 Christian Del Bianco. All rights reserved.
////////////////////////////////////////////////////////////////////////////////
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