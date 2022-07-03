using Kusto.Mirror.ConsoleApp.Database;
using Kusto.Mirror.ConsoleApp.Parameters;
using Kusto.Mirror.ConsoleApp.Storage.DeltaTable;
using System.Collections.Immutable;
using System.Diagnostics;

namespace Kusto.Mirror.ConsoleApp
{
    internal class MirrorOrchestration : IAsyncDisposable
    {
        private readonly IImmutableList<DeltaTableOrchestration> _orchestrations;

        public MirrorOrchestration(IEnumerable<DeltaTableOrchestration> orchestrations)
        {
            _orchestrations = orchestrations.ToImmutableArray();
        }

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
            var orchestrations = new List<DeltaTableOrchestration>();

            foreach (var db in databaseGroups)
            {
                Trace.WriteLine($"Initialize Database '{db.Gateway.DatabaseName}' schemas...");
                await db.Gateway.CreateMergeDatabaseObjectsAsync(ct);
                Trace.WriteLine($"Read Database '{db.Gateway.DatabaseName}' status...");

                var tableNames = db.Tables.Select(t => t.KustoTable);
                var tableParameterizationMap = db.Tables.ToImmutableDictionary(
                    t => t.KustoTable);
                var statusTables =
                    await TableStatus.LoadStatusTableAsync(db.Gateway, tableNames, ct);
                var tableOrchestrations = statusTables
                    .Select(t => new DeltaTableOrchestration(
                        t,
                        new DeltaTableGateway(
                            parameters.AuthenticationMode,
                            tableParameterizationMap[t.TableName].DeltaTableStorageUrl)));

                orchestrations.AddRange(tableOrchestrations);
            }

            return new MirrorOrchestration(orchestrations);
        }

        internal async Task RunAsync(CancellationToken ct)
        {
            var orchestrationTasks = _orchestrations.Select(o => o.RunAsync(ct));

            await Task.WhenAll(orchestrationTasks);
        }
    }
}