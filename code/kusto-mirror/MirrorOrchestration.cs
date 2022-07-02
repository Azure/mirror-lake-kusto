using Kusto.Mirror.ConsoleApp.Database;
using Kusto.Mirror.ConsoleApp.Parameters;
using System.Collections.Immutable;
using System.Diagnostics;

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
            Trace.WriteLine("Initialize Kusto Cluster connections...");

            var clusterGateway = await KustoClusterGateway.CreateAsync(
                parameters.AuthenticationMode,
                parameters.ClusterIngestionUri,
                version,
                requestDescription);
            var databaseGateways = parameters
                .DeltaTableParameterizations
                .Select(p => p.Database)
                .Distinct()
                .Select(db => new DatabaseGateway(clusterGateway, db))
                .ToImmutableArray();
            var statusTables = new List<StatusTable>(databaseGateways.Length);

            foreach (var db in databaseGateways)
            {
                Trace.WriteLine($"Initialize Database '{db.DatabaseName}' schemas...");
                await db.CreateMergeDatabaseObjectsAsync(ct);
                Trace.WriteLine($"Read Database '{db.DatabaseName}' status...");
                var statusTable = await StatusTable.LoadStatusTableAsync(db);

                statusTables.Add(statusTable);
            }

            return new MirrorOrchestration();
        }

        internal Task RunAsync(CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}