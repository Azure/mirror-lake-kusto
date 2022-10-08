using MirrorLakeKusto.Kusto;
using MirrorLakeKusto.Storage;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Reflection.Metadata;

namespace MirrorLakeKusto.Orchestrations
{
    internal class BlobStagingOrchestration
    {
        private static readonly TimeSpan DELAY_BETWEEN_PERSIST = TimeSpan.FromSeconds(5);

        private readonly DatabaseGateway _databaseGateway;
        private readonly TableDefinition _stagingTable;
        private readonly TableStatus _tableStatus;
        private readonly Uri _deltaTableStorageUrl;
        private readonly ConcurrentQueue<IEnumerable<TransactionItem>> _itemsToIngest;
        private readonly ConcurrentQueue<IEnumerable<TransactionItem>> _itemsToPersist
            = new ConcurrentQueue<IEnumerable<TransactionItem>>();
        //  Triggered when the items to ingest queue is empty (avoid sleeping to find out)
        private readonly TaskCompletionSource _ingestionQueueTask = new TaskCompletionSource();

        #region Constructors
        public static async Task EnsureAllStagedAsync(
            DatabaseGateway databaseGateway,
            TableDefinition stagingTable,
            TableStatus tableStatus,
            long startTxId,
            Uri deltaTableStorageUrl,
            CancellationToken ct)
        {
            var orchestration = new BlobStagingOrchestration(
                databaseGateway,
                stagingTable,
                tableStatus,
                startTxId,
                deltaTableStorageUrl);

            await orchestration.RunAsync(ct);
        }

        private BlobStagingOrchestration(
            DatabaseGateway databaseGateway,
            TableDefinition stagingTable,
            TableStatus tableStatus,
            long startTxId,
            Uri deltaTableStorageUrl)
        {
            var logs = tableStatus.GetBatch(startTxId);
            var itemsToIngest = logs.Adds;

            _databaseGateway = databaseGateway;
            _stagingTable = stagingTable;
            _tableStatus = tableStatus;
            _deltaTableStorageUrl = deltaTableStorageUrl;
            _itemsToIngest = new ConcurrentQueue<IEnumerable<TransactionItem>>(new[]
                {
                    itemsToIngest
                });
        }
        #endregion

        #region Orchestration
        private async Task RunAsync(CancellationToken ct)
        {
            var pipelineWidth = await ComputePipelineWidthAsync(ct);
            var ingestionTasks = Enumerable.Range(0, pipelineWidth)
                .Select(i => IngestItemsAsync(ct))
                .ToImmutableArray();
            var persistStatusTask = LoopPersistStatusAsync(ct);

            await Task.WhenAll(ingestionTasks);
            //  Signal end of ingestion
            _ingestionQueueTask.TrySetResult();
            //  This should stop immediately
            await persistStatusTask;
        }

        private async Task LoopPersistStatusAsync(CancellationToken ct)
        {
            while (!_ingestionQueueTask.Task.IsCompleted)
            {   //  First sleep a little
                await Task.WhenAll(_ingestionQueueTask.Task, Task.Delay(DELAY_BETWEEN_PERSIST));

                await PersistStatusAsync(ct);
            }
            //  Pickup items that might have slipped in racing condition
            await PersistStatusAsync(ct);
        }

        private async Task PersistStatusAsync(CancellationToken ct)
        {
            var items = DequeueAllItemStatusToPersist();

            if (items.Any())
            {
                var itemsList = items.SelectMany(i => i);

                await _tableStatus.PersistNewItemsAsync(itemsList, ct);
            }
        }

        private IImmutableList<IEnumerable<TransactionItem>> DequeueAllItemStatusToPersist()
        {
            var builder = ImmutableArray<IEnumerable<TransactionItem>>.Empty.ToBuilder();

            while (_itemsToPersist.Any())
            {
                if (_itemsToPersist.TryDequeue(out var items))
                {
                    builder.Add(items);
                }
            }

            return builder.ToImmutableArray();
        }

        private async Task IngestItemsAsync(CancellationToken ct)
        {
            while (_itemsToIngest.Any())
            {
                if (_itemsToIngest.TryDequeue(out var items))
                {
                    var urlList = items.Select(i => i.BlobPath!);
                    var urlListText = string.Join(
                        ", " + Environment.NewLine,
                        urlList.Select(u => $"'{u};impersonate'"));
                    var ingestionCommandText = @$"
.ingest into table {_stagingTable.Name}
(
    {urlListText}
) with
(
    format='parquet',
    ingestionMappingReference='Mapping'
)";
                    //  Ingest through Query Engine
                    var results = await _databaseGateway.ExecuteCommandAsync(
                        ingestionCommandText,
                        r => new
                        {
                            ExtentId = (Guid)r["ExtentId"],
                            ItemLoaded = (string)r["ItemLoaded"],
                            Duration = (TimeSpan)r["Duration"],
                            HasErrors = (bool)r["HasErrors"],
                            OperationId = (Guid)r["OperationId"]
                        },
                        ct);
                    var extentIds = results.Select(r => r.ExtentId).ToImmutableArray();
                    var newItems = items
                        .Select(i => i
                        .UpdateState(TransactionItemState.Staged)
                        .Clone(i => i.InternalState.AddInternalState!.StagingExtentIds = extentIds));

                    //  Queue updated items to persist
                    _itemsToPersist.Enqueue(newItems);
                }
            }
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
            var width = Math.Min(ingestionSlotCount, _itemsToIngest.Count);

            return width;
        }
        #endregion
    }
}