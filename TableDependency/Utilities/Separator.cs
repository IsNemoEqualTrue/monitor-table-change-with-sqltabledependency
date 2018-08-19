#region License
#endregion

using static System.String;

namespace TableDependency.Utilities
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
            _separator = Concat(Empty, separator);
            _startIndex = startIndex;
        }

        public string GetSeparator()
        {
            return _currentIndex++ >= _startIndex ? _separator : Empty;
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