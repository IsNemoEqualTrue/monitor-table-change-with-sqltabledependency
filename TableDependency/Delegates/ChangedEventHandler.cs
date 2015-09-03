using TableDependency.EventArgs;

namespace TableDependency.Delegates
{
    public delegate void ChangedEventHandler<T>(object sender, RecordChangedEventArgs<T> e);
}