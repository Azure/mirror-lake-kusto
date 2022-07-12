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
        private const string BLOB_PATH_COLUMN = "KM_BlobPath";
        private const string BLOB_ROW_NUMBER_COLUMN = "KM_Blob_RowNumber";
        private static readonly TimeSpan BETWEEN_TX_PROBE_DELAY = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan BETWEEN_EXTENT_PROBE_DELAY = TimeSpan.FromSeconds(5);

        private readonly TableStatus _tableStatus;
        private readonly DeltaTableGateway _deltaTableGateway;
        private readonly DatabaseGateway _databaseGateway;

        public DeltaTableOrchestration(
            TableStatus tableStatus,
            DeltaTableGateway deltaTableGateway,
            DatabaseGateway databaseGateway)
        {
            _tableStatus = tableStatus;
            _deltaTableGateway = deltaTableGateway;
            _databaseGateway = databaseGateway;
        }

        public string KustoDatabaseName => _tableStatus.DatabaseName;

        public string KustoTableName => _tableStatus.TableName;

        internal async Task RunAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                if (_tableStatus.IsBatchIncomplete)
                {
                    await ProcessTransactionBatchAsync(
                        _tableStatus.GetEarliestIncompleteBatch(),
                        ct);
                }
                else
                {
                    var currentTxId = _tableStatus.LastTxId;
                    var newLogs = await _deltaTableGateway.GetTransactionLogsAsync(
                        currentTxId + 1,
                        KustoDatabaseName,
                        KustoTableName,
                        ct);

                    if (newLogs.Any())
                    {
                        await PersistNewLogsAsync(newLogs, ct);
                    }
                    else
                    {
                        await Task.Delay(BETWEEN_TX_PROBE_DELAY, ct);
                    }
                }
            }
        }

        private async Task ProcessTransactionBatchAsync(
            TransactionLog logs,
            CancellationToken ct)
        {
            Trace.TraceInformation(
                $"Processing Transaction Batch {logs.AllItems.First().StartTxId} "
                + $"to {logs.AllItems.First().EndTxId}");

            if (logs.StartTxId == 0)
            {
                if (logs.Metadata == null)
                {
                    throw new InvalidOperationException("Transaction 0 should have meta data");
                }
                await EnsureTableSchemaAsync(logs.Metadata, ct);
                logs = _tableStatus.Refresh(logs);
            }
            var targetTable = await GetTableSchemaAsync(logs.Metadata, ct);
            var stagingTableSchema = GetStagingTableSchema(
                logs.StagingTable!.StagingTableName!,
                targetTable);
            var isStaging = await EnsureStagingTableAsync(
                stagingTableSchema,
                logs.StagingTable!,
                ct);

            if (isStaging)
            {
                await EnsureAllQueuedAsync(stagingTableSchema, logs.StartTxId, ct);
                await EnsureAllStagedAsync(stagingTableSchema, logs.StartTxId, ct);
                await EnsureAllLoadedAsync(stagingTableSchema, logs.StartTxId, ct);
                await DropStagingTableAsync(logs.StagingTable!, logs.StartTxId, ct);
                Trace.TraceInformation(
                    $"Table '{_tableStatus.TableName}', "
                    + $"tx {logs.StartTxId} to {logs.EndTxId} processed");
            }
            else
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
                    await ResetTransactionBatchAsync(logs, ct);
                }
            }
        }

        private async Task ResetTransactionBatchAsync(
            TransactionLog log,
            CancellationToken ct)
        {
            var stagingTable =
                log.StagingTable!.UpdateState(TransactionItemState.Initial);
            var resetAdds = log.Adds
                .Where(a => a.State != TransactionItemState.Initial)
                .Select(a => a.UpdateState(TransactionItemState.Initial));
            var template = log.AllItems.First();

            stagingTable.StagingTableName =
                CreateStagingTableName(stagingTable.StartTxId);

            Trace.TraceInformation(
                $"Reseting transaction batch {template.StartTxId} to {template.EndTxId}");
            await _tableStatus.PersistNewItemsAsync(resetAdds.Append(stagingTable), ct);
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
            TableDefinition stagingTable,
            long startTxId,
            CancellationToken ct)
        {
            var logs = _tableStatus.GetBatch(startTxId);
            var blobPathToRemove = logs.Removes
                .Select(i => i.BlobPath!)
                .Distinct()
                .ToImmutableArray();
            var removeBlobPathsTask = RemoveBlobPathsAsync(blobPathToRemove, ct);

            if (logs.Metadata != null)
            {
                await EnsureTableSchemaAsync(logs.Metadata, ct);
                await _tableStatus.PersistNewItemsAsync(
                    new[] { logs.Metadata.UpdateState(TransactionItemState.Done) }, ct);
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

            await _tableStatus.PersistNewItemsAsync(newAdded, ct);
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

        private Task RemoveBlobPathsAsync(
            IEnumerable<string> blobPathToRemove,
            CancellationToken ct)
        {
            return Task.CompletedTask;
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

        private async Task<TableDefinition> GetTableSchemaAsync(
            TransactionItem? metadata,
            CancellationToken ct)
        {
            IEnumerable<ColumnDefinition> columns;

            if (metadata != null && metadata.StartTxId == 0)
            {
                columns = metadata.Schema!;
            }
            else
            {
                var textSchema = await _databaseGateway.ExecuteCommandAsync(
                    $".show table {_tableStatus.TableName} schema as csl",
                    r => (string)r["Schema"],
                    ct);

                columns = textSchema
                    .First()
                    .Split(',')
                    .Select(t => t.Split(':'))
                    .Select(a => new ColumnDefinition { ColumnName = a[0], ColumnType = a[1] });
            }

            return new TableDefinition(_tableStatus.TableName, columns);
        }

        private TableDefinition GetStagingTableSchema(
            string stagingTableName,
            TableDefinition targetTable)
        {
            var moreColumns = targetTable.Columns
                .Append(new ColumnDefinition
                {
                    ColumnName = BLOB_PATH_COLUMN,
                    ColumnType = "string"
                })
                .Append(new ColumnDefinition
                {
                    ColumnName = BLOB_ROW_NUMBER_COLUMN,
                    ColumnType = "long"
                });

            return new TableDefinition(stagingTableName, moreColumns);
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
            var cachePolicyText = $".alter table {stagingTableSchema.Name} policy caching hot = 0d";
            //  Don't delete anything in the table
            var retentionPolicyText = @$".alter table {stagingTableSchema.Name} policy retention 
```
{{
  ""SoftDeletePeriod"": ""10000000:0:0:0""
}}
```";
            //  Staging table:  shouldn't be queried by normal users
            var restrictedViewPolicyText =
                $".alter table {stagingTableSchema.Name} policy restricted_view_access true";
            var commandText = @$"
.execute database script with (ContinueOnErrors=false, ThrowOnErrors=true) <|
{createTableText}

{retentionPolicyText}

{mergePolicyText}";
            //{cachePolicyText}
            //{restrictedViewPolicyText}";

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
                var ingestionMappings = CreateIngestionMappings(stagingTable);
                var queueTasks = toBeAdded
                    .Select(async item =>
                    {
                        await QueueItemAsync(stagingTable, item, ingestionMappings, ct);
                    })
                    .ToImmutableArray();
                var newItems = toBeAdded
                    .Select(item => item.UpdateState(TransactionItemState.QueuedForIngestion));

                Trace.TraceInformation($"Queuing {queueTasks.Count()} blobs for ingestion");
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

        private static ImmutableArray<ColumnMapping> CreateIngestionMappings(
            TableDefinition stagingTable)
        {
            var location =
                new Dictionary<string, string>() { { "Transform", "SourceLocation" } };
            var lineNumber =
                new Dictionary<string, string>() { { "Transform", "SourceLineNumber" } };
            var ingestionMappings = stagingTable
                .Columns
                .Select(c => new ColumnMapping()
                {
                    ColumnName = c.ColumnName,
                    ColumnType = c.ColumnType,
                    Properties = c.ColumnName == BLOB_PATH_COLUMN
                    ? location
                    : c.ColumnName == BLOB_ROW_NUMBER_COLUMN
                    ? lineNumber
                    : new Dictionary<string, string>()
                })
                .ToImmutableArray();

            return ingestionMappings;
        }

        private async Task EnsureTableSchemaAsync(TransactionItem metadata, CancellationToken ct)
        {
            if (metadata.State == TransactionItemState.Initial)
            {
                var schema = metadata.Schema!
                    .Append(new ColumnDefinition
                    {
                        ColumnName = BLOB_PATH_COLUMN,
                        ColumnType = "string"
                    })
                    .Append(new ColumnDefinition
                    {
                        ColumnName = BLOB_ROW_NUMBER_COLUMN,
                        ColumnType = "long"
                    });
                var columnsText = schema
                    .Select(c => $"['{c.ColumnName}']:{c.ColumnType}");
                var schemaText = string.Join(", ", columnsText);
                var createTableText = $".create-merge table {_tableStatus.TableName} ({schemaText})";
                var newMetadata = metadata.UpdateState(TransactionItemState.Done);

                Trace.TraceInformation(
                    "Updating schema of Kusto table "
                    + $"'{_tableStatus.DatabaseName}.{_tableStatus.TableName}'");

                await _databaseGateway.ExecuteCommandAsync(createTableText, r => 0, ct);
                await _tableStatus.PersistNewItemsAsync(new[] { newMetadata }, ct);
            }
        }

        private async Task PersistNewLogsAsync(
            IEnumerable<TransactionLog> newLogs,
            CancellationToken ct)
        {
            var mergedLogs = TransactionLog.Coalesce(newLogs);
            var templateItem = mergedLogs.AllItems.First();
            var startTxId = templateItem.StartTxId;
            var stagingTableName = CreateStagingTableName(startTxId);
            var stagingTableItem = TransactionItem.CreateStagingTableItem(
                templateItem.KustoDatabaseName,
                templateItem.KustoTableName,
                templateItem.StartTxId,
                templateItem.EndTxId,
                TransactionItemState.Initial,
                stagingTableName);
            var allItems = mergedLogs.AllItems.Append(stagingTableItem);

            await _tableStatus.PersistNewItemsAsync(allItems, ct);
        }

        private string CreateStagingTableName(long startTxId)
        {
            var uniqueId = DateTime.UtcNow.Ticks.ToString("x8");

            return $"KM_Staging_{_tableStatus.TableName}_{startTxId}_{uniqueId}";
        }
    }
}