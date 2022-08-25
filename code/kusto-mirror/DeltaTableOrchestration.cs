using Kusto.Cloud.Platform.Utils;
using Kusto.Data.Common;
using Kusto.Data.Exceptions;
using Kusto.Data.Ingestion;
using Kusto.Ingest;
using Kusto.Mirror.ConsoleApp.Kusto;
using Kusto.Mirror.ConsoleApp.Storage;
using Kusto.Mirror.ConsoleApp.Storage.DeltaLake;
using System.Collections.Immutable;
using System.Diagnostics;

namespace Kusto.Mirror.ConsoleApp
{
    internal class DeltaTableOrchestration
    {
        private const int EXTENT_PROBE_BATCH_SIZE = 5;
        private const int EXTENT_MOVE_BATCH_SIZE = 5;
        private const string INGEST_BY_PREFIX = "ingest-by:";
        private const string STAGED_TAG = "km_staged";
        private static readonly TimeSpan BETWEEN_TX_PROBE_DELAY = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan BETWEEN_EXTENT_PROBE_DELAY = TimeSpan.FromSeconds(5);

        private readonly TableStatus _tableStatus;
        private readonly DeltaTableGateway _deltaTableGateway;
        private readonly DatabaseGateway _databaseGateway;
        private readonly bool _continuousRun;
        private readonly bool _isFreeCluster;

        public DeltaTableOrchestration(
            TableStatus tableStatus,
            DeltaTableGateway deltaTableGateway,
            DatabaseGateway databaseGateway,
            bool continuousRun,
            bool isFreeCluster)
        {
            _tableStatus = tableStatus;
            _deltaTableGateway = deltaTableGateway;
            _databaseGateway = databaseGateway;
            _continuousRun = continuousRun;
            _isFreeCluster = isFreeCluster;
        }

        public string KustoDatabaseName => _tableStatus.DatabaseName;

        public string KustoTableName => _tableStatus.TableName;

        internal async Task RunAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                if (!_tableStatus.IsBatchIncomplete)
                {
                    var currentLog = _tableStatus.GetAll();
                    var newLogs = await _deltaTableGateway.GetNextTransactionLogAsync(
                        currentLog,
                        KustoDatabaseName,
                        KustoTableName,
                        ct);

                    if (newLogs != null)
                    {   //  Persists the logs and loop again to process them
                        await PersistNewLogsAsync(newLogs, ct);
                    }
                    else if (_continuousRun)
                    {   //  If continuous run, we wait a little before probing for new logs again
                        await Task.Delay(BETWEEN_TX_PROBE_DELAY, ct);
                    }
                    else
                    {   //  If not continuous log, that's the end of the road since there
                        //  isn't any new logs
                        return;
                    }
                }
                if (_tableStatus.IsBatchIncomplete)
                {
                    await ProcessTransactionBatchAsync(
                        _tableStatus.GetEarliestIncompleteBatchTxId(),
                        ct);
                }
            }
        }

        private async Task ProcessTransactionBatchAsync(long startTxId, CancellationToken ct)
        {
            var logs = _tableStatus.GetBatch(startTxId);

            Trace.TraceInformation(
                $"Processing Transaction Batch {logs.AllItems.First().StartTxId} "
                + $"to {logs.AllItems.First().EndTxId}");

            var mainTable = _tableStatus
                .GetTableDefinition(startTxId)
                .WithTrackingColumns();
            var stagingTable = mainTable.RenameTable(logs.StagingTable!.StagingTableName!);
            var isStaging = await EnsureStagingTableAsync(
                stagingTable,
                logs.StagingTable!,
                ct);

            if (!isStaging)
            {
                var notDone = logs.AllItems.Where(l => l.State != TransactionItemState.Done);

                if (notDone.Count() == 1)
                {
                    if (notDone.First().Action != TransactionItemAction.StagingTable)
                    {
                        throw new MirrorException("Should only have the staging table remain");
                    }
                    await _tableStatus.PersistNewItemsAsync(
                        new[] { logs.StagingTable!.UpdateState(TransactionItemState.Done) },
                        ct);
                }
                else
                {
                    await ResetTransactionBatchAsync(startTxId, ct);
                }
            }

            await LoadTransactionBatchAsync(startTxId, mainTable, stagingTable, ct);
        }

        private async Task LoadTransactionBatchAsync(
            long startTxId,
            TableDefinition mainTable,
            TableDefinition stagingTable,
            CancellationToken ct)
        {
            var logs = _tableStatus.GetBatch(startTxId);

            if (logs.StartTxId == 0)
            {
                if (logs.Metadata == null)
                {
                    throw new InvalidOperationException("Transaction 0 should have meta data");
                }
                await EnsureLandingTableSchemaAsync(stagingTable, logs.Metadata, ct);
                logs = _tableStatus.Refresh(logs);
            }
            await EnsureAllQueuedAsync(stagingTable, logs.StartTxId, ct);
            await EnsureAllStagedAsync(stagingTable, logs.StartTxId, ct);
            await EnsureAllLoadedAsync(mainTable, stagingTable, logs.StartTxId, ct);
            await DropStagingTableAsync(logs.StagingTable!, logs.StartTxId, ct);
            Trace.TraceInformation(
                $"Table '{_tableStatus.TableName}', "
                + $"tx {logs.StartTxId} to {logs.EndTxId} processed");
        }

        private async Task ResetTransactionBatchAsync(
            long startTxId,
            CancellationToken ct)
        {
            var log = _tableStatus.GetBatch(startTxId);
            var stagingTable =
                log.StagingTable!.UpdateState(TransactionItemState.Initial);
            var resetAdds = log.Adds
                .Where(a => a.State != TransactionItemState.Initial)
                .Select(a => a.UpdateState(TransactionItemState.Initial));
            var template = log.AllItems.First();

            stagingTable.StagingTableName =
                CreateStagingTableName(stagingTable.StartTxId);

            Trace.TraceInformation(
                $"Resetting transaction batch {template.StartTxId} to {template.EndTxId}");

            await _tableStatus.PersistNewItemsAsync(resetAdds.Append(stagingTable), ct);
            await ProcessTransactionBatchAsync(startTxId, ct);
        }

        private async Task EnsureAllStagedAsync(
            TableDefinition stagingTable,
            long startTxId,
            CancellationToken ct)
        {
            var logs = _tableStatus.GetBatch(startTxId);

            if (logs.Adds.Where(a => a.State == TransactionItemState.QueuedForIngestion).Any())
            {
                var itemMap = logs
                    .Adds
                    .ToImmutableDictionary(a => a.BlobPath!);
                //  Limit output, not to have a too heavy result set
                var showTableText = $@".show table {stagingTable.Name} extents
where tags !has '{STAGED_TAG}' and tags contains '{INGEST_BY_PREFIX}'
| project tostring(ExtentId), Tags
| take {EXTENT_PROBE_BATCH_SIZE}";
                var extents = await _databaseGateway.ExecuteCommandAsync(
                    showTableText,
                    d => new
                    {
                        ExtentId = (string)d["ExtentId"],
                        BlobPath = ((string)d["Tags"])
                        .Split(' ')
                        .Where(t => t.StartsWith(INGEST_BY_PREFIX))
                        .Select(t => t.Substring(INGEST_BY_PREFIX.Length))
                        .First()
                    },
                    ct);
                var extentsToStage = extents
                    .Where(e => itemMap.ContainsKey(e.BlobPath))
                    .ToImmutableArray();

                if (extentsToStage.Any())
                {
                    var itemsToUpdate = extentsToStage
                        .Select(e => new { Item = itemMap[e.BlobPath], e.ExtentId })
                        .Where(i => i.Item.State == TransactionItemState.QueuedForIngestion)
                        .Select(i => new
                        {
                            Item = i.Item.UpdateState(TransactionItemState.Staged),
                            i.ExtentId
                        })
                        .ToImmutableArray();

                    if (itemsToUpdate.Any())
                    {
                        //  We first persist the transaction items:
                        //  possible to have unmarked extents
                        await _tableStatus.PersistNewItemsAsync(
                            itemsToUpdate.Select(i => i.Item),
                            ct);
                    }

                    var extentIdsText =
                        string.Join(", ", extentsToStage.Select(e => $"'{e.ExtentId}'"));
                    var tagExtentsCommandText = $@".alter-merge extent tags ('{STAGED_TAG}') <|
print ExtentId=dynamic([{extentIdsText}])
| mv-expand ExtentId to typeof(string)";

                    await _databaseGateway.ExecuteCommandAsync(tagExtentsCommandText, r => 0, ct);
                }
                else
                {
                    await Task.Delay(BETWEEN_EXTENT_PROBE_DELAY, ct);
                }
                //  Recursive
                await EnsureAllStagedAsync(stagingTable, startTxId, ct);
            }
        }

        private async Task EnsureAllLoadedAsync(
            TableDefinition mainTable,
            TableDefinition stagingTable,
            long startTxId,
            CancellationToken ct)
        {
            var logs = _tableStatus.GetBatch(startTxId);
            var blobPathToRemove = logs.Removes
                .Select(i => i.BlobPath!)
                .Distinct()
                .ToImmutableArray();
            var removeBlobPathsTask = RemoveBlobPathsAsync(mainTable, blobPathToRemove, ct);

            if (logs.Metadata != null)
            {
                await EnsureLandingTableSchemaAsync(stagingTable, logs.Metadata, ct);
            }

            Trace.TraceInformation($"Loading extents in main table");
            await LoadExtentsAsync(stagingTable, ct);
            await removeBlobPathsTask;
            await DropTagsAsync(startTxId, ct);

            var newItems = logs.Adds.Concat(logs.Removes)
                .Select(item => item.UpdateState(TransactionItemState.Done));

            await _tableStatus.PersistNewItemsAsync(newItems, ct);
        }

        private async Task DropTagsAsync(long startTxId, CancellationToken ct)
        {
            var dropTagsCommandText = $@".drop extent tags <|
.show table {_tableStatus.TableName} extents where tags contains '{INGEST_BY_PREFIX}'";

            await _databaseGateway.ExecuteCommandAsync(dropTagsCommandText, r => 0, ct);

            var logs = _tableStatus.GetBatch(startTxId);
            var newAdded = logs.Adds
                .Select(item => item.UpdateState(TransactionItemState.Done));

            if (newAdded.Any())
            {
                await _tableStatus.PersistNewItemsAsync(newAdded, ct);
            }
        }

        private async Task LoadExtentsAsync(TableDefinition stagingTable, CancellationToken ct)
        {
            do
            {   //  Loop until we moved all extents
                var moveCommandText = $@".move extents to table {_tableStatus.TableName} <|
.show table {stagingTable.Name} extents where tags contains 'ingest-by'
| take {EXTENT_MOVE_BATCH_SIZE}";
                var results =
                    await _databaseGateway.ExecuteCommandAsync(moveCommandText, r => 0, ct);

                if (!results.Any())
                {
                    return;
                }
            }
            while (true);
        }

        private async Task RemoveBlobPathsAsync(
            TableDefinition mainTable,
            IEnumerable<string> blobPathToRemove,
            CancellationToken ct)
        {
            if (blobPathToRemove.Any())
            {
                var commandTextPrefix = @$".delete table {mainTable.Name} records <|
{mainTable.Name}";
                var pathColumn = mainTable.BlobPathColumnName;
                var predicates = blobPathToRemove
                    .Select(b => $"{Environment.NewLine}| where {pathColumn}=='{b}'");
                var commandText = commandTextPrefix + string.Join(string.Empty, predicates);

                await _databaseGateway.ExecuteCommandAsync(commandText, r => 0, ct);
            }
        }

        private async Task DropStagingTableAsync(
            TransactionItem stagingTable,
            long startTxId,
            CancellationToken ct)
        {
            var commandText = $".drop table {stagingTable.StagingTableName} ifexists";

            await _databaseGateway.ExecuteCommandAsync(commandText, r => 0, ct);

            var newSchemaItem = stagingTable.UpdateState(TransactionItemState.Done);

            await _tableStatus.PersistNewItemsAsync(new[] { newSchemaItem }, ct);
        }

        private async Task<bool> EnsureStagingTableAsync(
            TableDefinition stagingTableSchema,
            TransactionItem stagingTableItem,
            CancellationToken ct)
        {
            if (stagingTableItem.State == TransactionItemState.Initial)
            {
                await CreateStagingTableAsync(stagingTableSchema, ct);
                await _tableStatus.PersistNewItemsAsync(
                    new[] { stagingTableItem.UpdateState(TransactionItemState.Staged) },
                    ct);

                return true;
            }
            else
            {
                try
                {
                    await _databaseGateway.ExecuteCommandAsync(
                        $".show table {stagingTableSchema.Name}",
                        r => 0, ct);

                    return true;
                }
                catch (MirrorException ex)
                {
                    if (ex.InnerException is EntityNotFoundException)
                    {
                        return false;
                    }
                    else
                    {
                        throw;
                    }
                }
            }
        }

        private async Task CreateStagingTableAsync(
            TableDefinition stagingTableSchema,
            CancellationToken ct)
        {
            var schemaText = string.Join(
                ", ",
                stagingTableSchema.Columns.Select(c => $"['{c.ColumnName}']:{c.ColumnType}"));
            var createTableText = $".create-merge table {stagingTableSchema.Name} ({schemaText})";
            //  Disable merge policy not to run after phantom extents
            var mergePolicyText = @$".alter table {stagingTableSchema.Name} policy merge
```
{{
  ""AllowRebuild"": false,
  ""AllowMerge"": false
}}
```";
            //  Don't cache anything not to potentially overload the cache with big historical data
            var cachePolicyText =
                _isFreeCluster
                ? string.Empty
                : $".alter table {stagingTableSchema.Name} policy caching hot = 0d";
            //  Don't delete anything in the table
            var retentionPolicyText =
                _isFreeCluster
                ? string.Empty
                : @$".alter table {stagingTableSchema.Name} policy retention 
```
{{
  ""SoftDeletePeriod"": ""10000000:0:0:0""
}}
```";
            //  Staging table:  shouldn't be queried by normal users
            var restrictedViewPolicyText =
                _isFreeCluster
                ? string.Empty
                : $".alter table {stagingTableSchema.Name} policy restricted_view_access true";
            var commandText = @$"
.execute database script with (ContinueOnErrors=false, ThrowOnErrors=true) <|
{createTableText}

{retentionPolicyText}

{mergePolicyText}

{cachePolicyText}

{restrictedViewPolicyText}";

            await _databaseGateway.ExecuteCommandAsync(commandText, r => 0, ct);
        }

        private async Task EnsureAllQueuedAsync(
            TableDefinition stagingTable,
            long startTxId,
            CancellationToken ct)
        {
            var logs = _tableStatus.GetBatch(startTxId);
            var toBeAdded = logs
                .Adds
                .Where(i => i.State == TransactionItemState.Initial);

            if (toBeAdded.Any())
            {
                Trace.TraceInformation($"Queuing {toBeAdded.Count()} blobs for ingestion");

                var queueTasks = toBeAdded
                    .Select(async item =>
                    {
                        if (item.PartitionValues == null)
                        {
                            throw new ArgumentNullException(nameof(item.PartitionValues));
                        }

                        var ingestionMappings = stagingTable.CreateIngestionMappings(
                            item.BlobPath!,
                            item.PartitionValues!);

                        await QueueItemAsync(stagingTable, item, ingestionMappings, ct);
                    })
                    .ToImmutableArray();
                var newItems = toBeAdded
                    .Select(item => item.UpdateState(TransactionItemState.QueuedForIngestion));

                await Task.WhenAll(queueTasks);
                await _tableStatus.PersistNewItemsAsync(newItems, ct);
            }
        }

        private async Task QueueItemAsync(
            TableDefinition stagingTable,
            TransactionItem item,
            IEnumerable<ColumnMapping> ingestionMappings,
            CancellationToken ct)
        {
            if (item.BlobPath == null)
            {
                throw new InvalidOperationException(
                    $"{nameof(item.BlobPath)} shouldn't be null here");
            }
            var fullBlobath = Path.Combine(
                $"{_deltaTableGateway.DeltaTableStorageUrl}/",
                item.BlobPath);
            var properties = _databaseGateway.GetIngestionProperties(stagingTable.Name);

            properties.Format = DataSourceFormat.parquet;
            properties.IngestionMapping = new IngestionMapping()
            {
                IngestionMappings = ingestionMappings,
                IngestionMappingKind = IngestionMappingKind.Parquet
            };
            properties.IngestIfNotExists = new[] { item.BlobPath };
            properties.IngestByTags = new[] { item.BlobPath };

            await _databaseGateway.QueueIngestionAsync(
                new Uri(fullBlobath),
                properties,
                ct);
        }

        private async Task EnsureLandingTableSchemaAsync(
            TableDefinition stagingTable,
            TransactionItem metadata,
            CancellationToken ct)
        {
            if (metadata.State == TransactionItemState.Initial)
            {
                var createTableText = $".create-merge table {_tableStatus.TableName}"
                    + $" ({stagingTable.KustoSchema})";

                Trace.TraceInformation(
                    "Updating schema of Kusto table "
                    + $"'{_tableStatus.DatabaseName}.{_tableStatus.TableName}'");

                await _databaseGateway.ExecuteCommandAsync(createTableText, r => 0, ct);
                await _tableStatus.PersistNewItemsAsync(
                    new[] { metadata.UpdateState(TransactionItemState.Done) },
                    ct);
            }
        }

        private async Task PersistNewLogsAsync(TransactionLog newLogs, CancellationToken ct)
        {
            var templateItem = newLogs.AllItems.First();
            var startTxId = templateItem.StartTxId;
            var stagingTableName = CreateStagingTableName(startTxId);
            var stagingTableItem = TransactionItem.CreateStagingTableItem(
                templateItem.KustoDatabaseName,
                templateItem.KustoTableName,
                templateItem.StartTxId,
                templateItem.EndTxId,
                TransactionItemState.Initial,
                stagingTableName);
            var allItems = newLogs.AllItems.Append(stagingTableItem);

            await _tableStatus.PersistNewItemsAsync(allItems, ct);
        }

        private string CreateStagingTableName(long startTxId)
        {
            var uniqueId = DateTime.UtcNow.Ticks.ToString("x8");

            return $"KM_Staging_{_tableStatus.TableName}_{startTxId}_{uniqueId}";
        }
    }
}