#region License
#endregion

using System;

namespace TableDependency.SqlClient.Base.Utilities
{
    public class Separator
    {
        private int _currentIndex = 1;
        private int _startIndex;
        private string _separator;

        public Separator(string separator) : this(1, separator)
        {
        }

        public Separator(int startIndex, string separator)
        {
            _separator = separator;
            _startIndex = startIndex;
        }

        public Separator(int startIndex, char separator)
        {
            _separator = String.Concat(String.Empty, separator);
            _startIndex = startIndex;
        }

        public string GetSeparator()
        {
            return _currentIndex++ >= _startIndex ? _separator : String.Empty;
        }

        public void Reset()
        {
            _currentIndex = 1;
        }

        public void Reset(int startIndex, string separator)
        {
            _currentIndex = 1;
            _separator = separator;
            _startIndex = startIndex;
        }

        public void Reset(int startIndex)
        {
            _currentIndex = 1;
            _startIndex = startIndex;
        }
    }
}