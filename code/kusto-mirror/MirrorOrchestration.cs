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
            var databaseGroups = parameters
                .DeltaTableParameterizations
                .GroupBy(p => p.Database)
                .Select(g => new
                {
                    Gateway = new DatabaseGateway(clusterGateway, g.Key),
                    Tables = g
                })
                .ToImmutableArray();
            //var statusTables = new List<StatusTable>(databaseGroups.Length);

            foreach (var db in databaseGroups)
            {
                Trace.WriteLine($"Initialize Database '{db.Gateway.DatabaseName}' schemas...");
                await db.Gateway.CreateMergeDatabaseObjectsAsync(ct);
                Trace.WriteLine($"Read Database '{db.Gateway.DatabaseName}' status...");

                var tableNames = db.Tables.Select(t => t.KustoTable);
                var statusTables =
                    await TableStatus.LoadStatusTableAsync(db.Gateway, tableNames, ct);
            }

            return new MirrorOrchestration();
        }

        internal Task RunAsync(CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}