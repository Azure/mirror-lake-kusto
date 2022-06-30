using Kusto.Mirror.ConsoleApp.Parameters;
using Kusto.Mirror.Database;
using System.Collections.Immutable;

namespace Kusto.Mirror.ConsoleApp
{
    internal class MirrorOrchestration : IAsyncDisposable
    {
        ValueTask IAsyncDisposable.DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

        internal static async Task<MirrorOrchestration> CreationOrchestrationAsync(
            MainParameterization parameters,
            string version,
            string? requestDescription,
            CancellationToken ct)
        {
            var clusterGateway = new KustoClusterGateway(
                parameters.ClusterQueryUri,
                version,
                requestDescription);
            var databaseManagers = parameters
                .DeltaTableParameterizations
                .Select(p => p.Database)
                .Distinct()
                .Select(db => new DatabaseGateway(clusterGateway, db))
                .ToImmutableArray();

            foreach(var db in databaseManagers)
            {
                await db.CreateMergeDatabaseObjectsAsync(ct);
            }

            throw new NotImplementedException();
        }

        internal Task RunAsync(CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}