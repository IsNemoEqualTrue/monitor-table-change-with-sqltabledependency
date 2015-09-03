using TableDependency.Delegates;

namespace TableDependency
{
    public interface ITableDependency<T>
    {
        event ChangedEventHandler<T> OnChanged;
        event ErrorEventHandler OnError;

        void Start(int timeOut = 120, int watchDogTimeOut = 180);
        void Stop();
    }
}