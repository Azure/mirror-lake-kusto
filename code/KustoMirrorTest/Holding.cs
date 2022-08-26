namespace KustoMirrorTest
{
    public class Holding<T> : IAsyncDisposable
    {
        private readonly Func<Task> _disposeAsyncFunc;

        public Holding(T value, Func<Task> disposeAsyncFunc)
        {
            Value = value;
            _disposeAsyncFunc = disposeAsyncFunc;
        }

        public T Value { get; }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await _disposeAsyncFunc();
        }
    }
}