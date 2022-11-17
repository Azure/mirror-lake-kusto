using Azure.Core;
using Azure.Identity;
using Kusto.Data;
using MirrorLakeKusto.Kusto;
using MirrorLakeKusto.Parameters;
using MirrorLakeKusto.Storage;
using MirrorLakeKusto.Storage.DeltaLake;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;

namespace MirrorLakeKusto.Orchestrations
{
    internal class MirrorOrchestration : IAsyncDisposable
    {
        private readonly IImmutableList<DeltaTableOrchestration> _orchestrations;

        #region Constructors
        public MirrorOrchestration(IEnumerable<DeltaTableOrchestration> orchestrations)
        {
            _orchestrations = orchestrations.ToImmutableArray();
        }

        internal static async Task<MirrorOrchestration> CreationOrchestrationAsync(
            MainParameterization parameters,
            string version,
            string? requestDescription,
            CancellationToken ct)
        {
            var storageCredentials = CreateNonSasStorageCredentials(
                parameters.ClusterIngestionConnectionString,
                parameters.ForceBrowserAuth);
            var globalTableStatusTask = GlobalTableStatus.RetrieveAsync(
                parameters.CheckpointBlobFolderUrl,
                storageCredentials,
                parameters.DeltaTableParameterizations.Select(d => d.KustoTable),
                ct);
            var clusterGateway = await KustoClusterGateway.CreateAsync(
                parameters.ClusterIngestionConnectionString,
                storageCredentials,
                version,
                requestDescription);
            var isFreeCluster = await clusterGateway.IsFreeClusterAsync(ct);
            var databaseGroups = parameters
                .DeltaTableParameterizations
                .GroupBy(p => p.Database)
                .Select(g => new
                {
                    Gateway = new DatabaseGateway(clusterGateway, g.Key),
                    Tables = g
                })
                .ToImmutableArray();
            var globalTableStatus = await globalTableStatusTask;
            var orchestrations = new List<DeltaTableOrchestration>();

            foreach (var db in databaseGroups)
            {
                Trace.TraceInformation($"Initialize Database '{db.Gateway.DatabaseName}' schemas...");
                await db.Gateway.CreateMergeDatabaseObjectsAsync(
                    globalTableStatus.CheckpointUri,
                    ct);
                Trace.TraceInformation($"Read Database '{db.Gateway.DatabaseName}' status...");

                var tableNames = db.Tables.Select(t => t.KustoTable);
                var tableParameterizationMap = db.Tables.ToImmutableDictionary(
                    t => t.KustoTable);
                var tableOrchestrations = tableNames
                    .Select(t => new DeltaTableOrchestration(
                        db.Gateway.DatabaseName,
                        globalTableStatus[t],
                        new DeltaTableGateway(
                            storageCredentials,
                            tableParameterizationMap[t].DeltaTableStorageUrl),
                        db.Gateway,
                        tableParameterizationMap[t].CreationTime,
                        tableParameterizationMap[t].GoBack,
                        parameters.ContinuousRun,
                        isFreeCluster));

                orchestrations.AddRange(tableOrchestrations);
            }

            return new MirrorOrchestration(orchestrations);
        }

        private static TokenCredential CreateNonSasStorageCredentials(
            string clusterIngestionConnectionString,
            bool forceBrowserAuth)
        {
            if (Uri.TryCreate(clusterIngestionConnectionString, UriKind.Absolute, out _))
            {   //  Default, if no credentials are provided
                if(forceBrowserAuth)
                {
                    return new InteractiveBrowserCredential();
                }
                else
                {
                return new DefaultAzureCredential();
                }
            }
            else
            {
                var builder = new KustoConnectionStringBuilder(clusterIngestionConnectionString);

                if (!string.IsNullOrWhiteSpace(builder.Authority)
                    && !string.IsNullOrWhiteSpace(builder.ApplicationClientId)
                    && !string.IsNullOrWhiteSpace(builder.ApplicationKey))
                {
                    return new ClientSecretCredential(
                        builder.Authority,
                        builder.ApplicationClientId,
                        builder.ApplicationKey);
                }
                else
                {
                    throw new MirrorException("Connection string unsupported");
                }
            }
        }
        #endregion

        ValueTask IAsyncDisposable.DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

        internal async Task RunAsync(CancellationToken ct)
        {
            var orchestrationTasks = _orchestrations.Select(o => o.RunAsync(ct));

            await Task.WhenAll(orchestrationTasks);
        }
    }
}