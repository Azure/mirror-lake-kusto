using Kusto.Mirror.ConsoleApp.Parameters;

namespace Kusto.Mirror.ConsoleApp
{
    internal class MirrorOrchestration : IAsyncDisposable
    {
        ValueTask IAsyncDisposable.DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

        internal static Task<MirrorOrchestration> CreationOrchestrationAsync(
            MainParameterization parameters)
        {
            throw new NotImplementedException();
        }

        internal Task RunAsync()
        {
            throw new NotImplementedException();
        }
    }
}