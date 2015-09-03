using TableDependency.Enums;

namespace TableDependency.EventArgs
{
    public abstract class RecordChangedEventArgs<T> : System.EventArgs
    {
        public abstract T Entity { get; protected set; }
        public abstract ChangeType ChangeType { get; protected set; }
        public abstract string MessageType { get; protected set; }
    }
}