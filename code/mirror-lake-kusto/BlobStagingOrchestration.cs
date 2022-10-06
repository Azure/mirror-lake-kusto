using MirrorLakeKusto.Kusto;
using MirrorLakeKusto.Storage;
using System.Collections.Concurrent;

namespace MirrorLakeKusto
{
    internal class BlobStagingOrchestration
    {
        private readonly DatabaseGateway _databaseGateway;
        private readonly TableDefinition _stagingTable;
        private readonly TableStatus _tableStatus;
        private readonly ConcurrentQueue<IEnumerable<TransactionItem>> _itemsBatchToIngest;

        #region Constructors
        public static async Task EnsureAllStagedAsync(
            DatabaseGateway databaseGateway,
            TableDefinition stagingTable,
            TableStatus tableStatus,
            long startTxId,
            CancellationToken ct)
        {
            var orchestration = new BlobStagingOrchestration(
                databaseGateway,
                stagingTable,
                tableStatus,
                startTxId);

            await orchestration.RunAsync(ct);
        }

        private BlobStagingOrchestration(
            DatabaseGateway databaseGateway,
            TableDefinition stagingTable,
            TableStatus tableStatus,
            long startTxId)
        {
            var logs = tableStatus.GetBatch(startTxId);
            var itemsToIngest = logs.Adds;

            _databaseGateway = databaseGateway;
            _stagingTable = stagingTable;
            _tableStatus = tableStatus;
            _itemsBatchToIngest = new ConcurrentQueue<IEnumerable<TransactionItem>>(new[]
                {
                    itemsToIngest
                });
        }
        #endregion

        #region Orchestration
        private async Task RunAsync(CancellationToken ct)
        {
            var pipelineWidth = await ComputePipelineWidthAsync(ct);

            throw new NotImplementedException();
        }

        private async Task<int> ComputePipelineWidthAsync(CancellationToken ct)
        {
            var ingestionSlots = await _databaseGateway.ExecuteCommandAsync(
                @".show capacity
| where Resource == ""Ingestions""
| project Total",
                r => (long)r[0],
                ct);
            var ingestionSlotCount = (int)ingestionSlots.First();
            var width = Math.Min(ingestionSlotCount, _itemsBatchToIngest.Count);

            return width;
        }
        #endregion
    }
}