using Azure.Core;
using Azure.Identity;
using Kusto.Mirror.ConsoleApp.Kusto;
using Kusto.Mirror.ConsoleApp.Parameters;
using Kusto.Mirror.ConsoleApp.Storage;
using Kusto.Mirror.ConsoleApp.Storage.DeltaLake;
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
            Trace.WriteLine("Initialize Storage connections...");

            var storageCredentials = CreateStorageCredentials(parameters.AuthenticationMode);
            var globalTableStatus = await GlobalTableStatus.RetrieveAsync(
                parameters.CheckpointBlobUrl,
                storageCredentials,
                ct);

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
                await db.Gateway.CreateMergeDatabaseObjectsAsync(
                    parameters.CheckpointBlobUrl,
                    ct);
                Trace.WriteLine($"Read Database '{db.Gateway.DatabaseName}' status...");

                var tableNames = db.Tables.Select(t => t.KustoTable);
                var tableParameterizationMap = db.Tables.ToImmutableDictionary(
                    t => t.KustoTable);
                var tableOrchestrations = tableNames
                    .Select(t => new DeltaTableOrchestration(
                        globalTableStatus.GetSingleTableStatus(db.Gateway.DatabaseName, t),
                        new DeltaTableGateway(
                            storageCredentials,
                            tableParameterizationMap[t].DeltaTableStorageUrl),
                        db.Gateway));

                orchestrations.AddRange(tableOrchestrations);
            }

            return new MirrorOrchestration(orchestrations);
        }

        private static TokenCredential CreateStorageCredentials(
            AuthenticationMode authenticationMode)
        {
            switch (authenticationMode)
            {
                case AuthenticationMode.AppSecret:
                    throw new NotSupportedException();
                case AuthenticationMode.AzCli:
                    return new AzureCliCredential();
                case AuthenticationMode.Browser:
                    return new InteractiveBrowserCredential();

                default:
                    throw new NotSupportedException(
                        $"Unsupported authentication mode '{authenticationMode}'");
            }
        }

        internal async Task RunAsync(CancellationToken ct)
        {
            var orchestrationTasks = _orchestrations.Select(o => o.RunAsync(ct));

            await Task.WhenAll(orchestrationTasks);
        }
    }
}