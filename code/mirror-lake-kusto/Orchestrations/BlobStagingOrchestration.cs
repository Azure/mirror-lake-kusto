using MirrorLakeKusto.Kusto;
using MirrorLakeKusto.Storage;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Reflection.Metadata;
using YamlDotNet.Core.Tokens;

namespace MirrorLakeKusto.Orchestrations
{
    internal class BlobStagingOrchestration
    {
        #region Inner types
        private class PartitionValuesComparer
            : IEqualityComparer<IImmutableDictionary<string, string>>
        {
            bool IEqualityComparer<IImmutableDictionary<string, string>>.Equals(
                IImmutableDictionary<string, string>? x,
                IImmutableDictionary<string, string>? y)
            {
                if ((x == null && y != null) || (x != null && y == null))
                {
                    return false;
                }
                else if (x == null || y == null)
                {
                    return true;
                }
                else if (x.Count != y.Count)
                {
                    return false;
                }
                else
                {
                    foreach (var pairX in x)
                    {
                        if (!y.TryGetValue(pairX.Key, out var valueY)
                            || pairX.Value != valueY)
                        {
                            return false;
                        }
                    }

                    return true;
                }
            }

            int IEqualityComparer<IImmutableDictionary<string, string>>.GetHashCode(
                IImmutableDictionary<string, string> obj)
            {
                if (obj.Any())
                {
                    //  XOR all hash codes
                    var hashcode = obj
                        .Select(p => p.Key.GetHashCode() ^ p.Value.GetHashCode())
                        .Aggregate((h1, h2) => h1 ^ h2);

                    return hashcode;
                }
                else
                {
                    return 0;
                }
            }
        }
        #endregion

        private static readonly TimeSpan DELAY_BETWEEN_PERSIST = TimeSpan.FromSeconds(5);

        private readonly DatabaseGateway _databaseGateway;
        private readonly TableDefinition _stagingTable;
        private readonly TableStatus _tableStatus;
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
            var nonEmptyPartitions = itemsToIngest
                .Where(i => i.PartitionValues != null)
                .GroupBy(i => i.PartitionValues!, new PartitionValuesComparer());
            var emptyPartition = itemsToIngest
                .Where(i => i.PartitionValues == null);

            _databaseGateway = databaseGateway;
            _stagingTable = stagingTable;
            _tableStatus = tableStatus;
            _itemsToIngest = new ConcurrentQueue<IEnumerable<TransactionItem>>(
                emptyPartition.Any()
                ? nonEmptyPartitions.Append(emptyPartition)
                : nonEmptyPartitions);
        }
        #endregion

        #region Orchestration
        private async Task RunAsync(CancellationToken ct)
        {
            Trace.WriteLine($"Staging {_itemsToIngest.SelectMany(i => i).Count()} blobs");

            var pipelineWidth = await ComputePipelineWidthAsync(ct);
            var ingestionTasks = Enumerable.Range(0, pipelineWidth)
                .Select(i => IngestItemsLoopAsync(ct))
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

        private async Task IngestItemsLoopAsync(CancellationToken ct)
        {
            while (_itemsToIngest.Any())
            {
                if (_itemsToIngest.TryDequeue(out var items))
                {
                    var emptyItems = items
                        .Where(i => i.RecordCount == 0)
                        .ToImmutableArray();
                    var nonEmptyItems = items
                        .Where(i => i.RecordCount != 0)
                        .ToImmutableArray();
                    var extentIds = await IngestItemsAsync(nonEmptyItems, ct);
                    var extentBlobMap = await ComputeExtentBlobMapAsync(extentIds, ct);
                    var newNonEmptyItems = nonEmptyItems
                        .Select(i => i
                            .UpdateState(TransactionItemState.Staged)
                            .Clone(i =>
                            {
                                var mapValue = extentBlobMap[i.BlobPath!];

                                i.InternalState.AddInternalState!.StagingExtentId = mapValue.extentId;
                                i.InternalState.AddInternalState!.IngestionTime = mapValue.ingestionTime;
                            }));
                    var newEmptyItems = emptyItems
                        .Select(i => i.UpdateState(TransactionItemState.Done));
                    var newItems = newNonEmptyItems.Concat(newEmptyItems).ToImmutableArray();

                    //  Queue updated items to persist
                    _itemsToPersist.Enqueue(newItems);
                }
            }
        }

        private async Task<IImmutableDictionary<Uri, (Guid extentId, string ingestionTime)>> ComputeExtentBlobMapAsync(
            IEnumerable<Guid> extentIds,
            CancellationToken ct)
        {
            var extentIdsText = string.Join(", ", extentIds.Select(i => $"'{i}'"));
            var queryText = @$"{_stagingTable.Name}
| where extent_id() in ({extentIdsText})
| summarize by
    ExtentId = extent_id(),
    BlobPath = {_stagingTable.BlobPathColumnName},
    IngestionTime = tostring(ingestion_time())";
            var aggregateResults = await _databaseGateway.ExecuteQueryAsync(
                queryText,
                r => new
                {
                    ExtentId = (Guid)r["ExtentId"],
                    BlobPath = (string)r["BlobPath"],
                    IngestionTime = (string)r["IngestionTime"]
                },
                ct);
            var map = aggregateResults
                .Select(r => new
                {
                    Key = new Uri(r.BlobPath),
                    Value = (r.ExtentId, r.IngestionTime)
                })
                .ToImmutableDictionary(r => r.Key, r => r.Value);

            return map;
        }

        private async Task<ImmutableArray<Guid>> IngestItemsAsync(
            IEnumerable<TransactionItem> items,
            CancellationToken ct)
        {
            var urlList = items.Select(i => i.BlobPath!);
            var urlListText = string.Join(
                ", " + Environment.NewLine,
                urlList.Select(u => $"'{u};impersonate'"));
            var sampleItem = items.First();
            var ingestionMappingText = CreateIngestionMappingText(
                sampleItem.PartitionValues!);
            var ingestionCommandText = @$"
.ingest into table {_stagingTable.Name}
(
    {urlListText}
) with
(
    format='parquet',
    ingestionMapping=
```
[
{ingestionMappingText}
]
```
)";
            //  Ingest through Query Engine
            var ingestResults = await _databaseGateway.ExecuteCommandAsync(
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
            var extentIds = ingestResults.Select(r => r.ExtentId).ToImmutableArray();

            return extentIds;
        }

        private string CreateIngestionMappingText(
            IImmutableDictionary<string, string> partitionValues)
        {
            var simpleColumnsMappingText = _stagingTable.Columns
                .Where(c => c.ColumnName != _stagingTable.BlobPathColumnName)
                .Where(c => partitionValues == null
                || !partitionValues.ContainsKey(c.ColumnName))
                .Select(c => @$"{{""Column"": ""{c.ColumnName}"", 
    ""Properties"": {{""Path"": ""$.{c.ColumnName}""}} }}");
            var partitionColumnsMappingText = partitionValues
                .Where(c => c.Value != null)
                .Select(c => $@"{{
    ""Column"": ""{c.Key}"",
    ""Properties"":
    {{
        ""ConstValue"": ""{c.Value}""
    }}
}}");
            var blobPathColumnMappingText = $@"{{
    ""Column"": ""{_stagingTable.BlobPathColumnName}"",
    ""Properties"":
    {{
        ""Path"": ""$.{_stagingTable.BlobPathColumnName}"",
        ""Transform"": ""SourceLocation""
    }}
}}";
            var combinedMappingText = string.Join(
                ", ",
                simpleColumnsMappingText
                .Concat(partitionColumnsMappingText)
                .Append(blobPathColumnMappingText));

            return combinedMappingText;
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