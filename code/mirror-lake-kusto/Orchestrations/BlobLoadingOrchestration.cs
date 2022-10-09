using MirrorLakeKusto.Kusto;
using MirrorLakeKusto.Storage;
using System.Collections.Concurrent;
using System.Collections.Immutable;

namespace MirrorLakeKusto.Orchestrations
{
    internal class BlobLoadingOrchestration
    {
        #region Constructors
        public static async Task EnsureAllLoadedAsync(
            DatabaseGateway databaseGateway,
            TableDefinition stagingTable,
            TableStatus tableStatus,
            long startTxId,
            Uri deltaTableStorageUrl,
            CancellationToken ct)
        {
            var orchestration = new BlobLoadingOrchestration(
                databaseGateway,
                stagingTable,
                tableStatus,
                startTxId,
                deltaTableStorageUrl);

            await orchestration.RunAsync(ct);
        }

        private BlobLoadingOrchestration(
            DatabaseGateway databaseGateway,
            TableDefinition stagingTable,
            TableStatus tableStatus,
            long startTxId,
            Uri deltaTableStorageUrl)
        {
        }
        #endregion

        #region Orchestration
        private async Task RunAsync(CancellationToken ct)
        {
            await Task.CompletedTask;

            throw new NotImplementedException();
        }
        #endregion
    }
}