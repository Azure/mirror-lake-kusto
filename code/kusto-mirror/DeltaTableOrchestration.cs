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
        private const string BLOB_PATH_COLUMN = "KM_BlobPath";
        private const string BLOB_ROW_NUMBER_COLUMN = "KM_Blob_RowNumber";

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
                    await ProcessTransactionBatchAsync(ct);
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
                        throw new NotImplementedException();
                    }
                }
            }
        }

        private async Task ProcessTransactionBatchAsync(CancellationToken ct)
        {
            var log = _tableStatus.GetEarliestIncompleteBatch();
            var startTxId = log.AllItems.First().StartTxId;

            Trace.WriteLine(
                $"Processing Transaction Batch {log.AllItems.First().StartTxId} "
                + $"to {log.AllItems.First().EndTxId}");
            if (startTxId == 0)
            {
                if (log.Metadata == null)
                {
                    throw new InvalidOperationException("Transaction 0 should have meta data");
                }
                await EnsureTableSchemaAsync(log.Metadata, ct);
                log = _tableStatus.GetEarliestIncompleteBatch();
            }
            var targetTable = await GetTableSchemaAsync(log.Metadata, ct);
            var stagingTableSchema = GetStagingTableSchema(
                log.StagingTable!.StagingTableName!,
                targetTable);
            var isStaging = await EnsureStagingTableAsync(
                stagingTableSchema,
                log.StagingTable!,
                ct);

            if (isStaging)
            {
                await EnsureAllQueuedAsync(stagingTableSchema, log.Adds, ct);
                log = _tableStatus.GetEarliestIncompleteBatch();
                await EnsureAllStagedAsync(stagingTableSchema, log.Adds, ct);
                log = _tableStatus.GetEarliestIncompleteBatch();
                await EnsureAllLoadedAsync(stagingTableSchema, log, ct);
                await DropStagingTableAsync(stagingTableSchema, ct);
            }
            else
            {
                await ResetTransactionBatchAsync(log, ct);
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

            Trace.WriteLine(
                $"Reseting transaction batch {template.StartTxId} to {template.EndTxId}");
            await _tableStatus.PersistNewItemsAsync(resetAdds.Append(stagingTable), ct);
        }

        private Task EnsureAllStagedAsync(
            TableDefinition stagingTable,
            IEnumerable<TransactionItem> adds,
            CancellationToken ct)
        {
            throw new NotImplementedException();
        }

        private async Task EnsureAllLoadedAsync(
            TableDefinition stagingTable,
            TransactionLog log,
            CancellationToken ct)
        {
            var extentIds = log.Adds
                .Select(i => i.ExtentId!)
                .Distinct()
                .ToImmutableArray();
            var blobPathToRemove = log.Removes
                .Select(i => i.BlobPath!)
                .Distinct()
                .ToImmutableArray();
            var removeBlobPathsTask = RemoveBlobPathsAsync(blobPathToRemove, ct);

            if (log.Metadata != null)
            {
                await EnsureTableSchemaAsync(log.Metadata, ct);
            }

            await LoadExtentsAsync(stagingTable, extentIds, ct);
            await removeBlobPathsTask;

            var newAdded = log.Adds
                .Select(item => item.UpdateState(TransactionItemState.Done));
            var newRemoved = log.Removes
                .Select(item => item.UpdateState(TransactionItemState.Done));

            await _tableStatus.PersistNewItemsAsync(newAdded.Concat(newRemoved), ct);
        }

        private Task LoadExtentsAsync(
            TableDefinition stagingTable,
            IEnumerable<string> extentIds,
            CancellationToken ct)
        {
            throw new NotImplementedException();
        }

        private Task RemoveBlobPathsAsync(
            IEnumerable<string> blobPathToRemove,
            CancellationToken ct)
        {
            throw new NotImplementedException();
        }

        private async Task DropStagingTableAsync(
            TableDefinition stagingTable,
            CancellationToken ct)
        {
            var commandText = $".drop table {stagingTable.Name}";
            await _databaseGateway.ExecuteCommandAsync(commandText, r => 0, ct);
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
                catch (EntityNotFoundException)
                {
                    return false;
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
            var ingestionBatchingPolicyText = @$".alter table {stagingTableSchema.Name} policy ingestionbatching
```
{{
  ""MaximumBatchingTimeSpan"":""0:0:10""
}}
```";
            var commandText = @$"
.execute database script with (ContinueOnErrors=false, ThrowOnErrors=true) <|
{createTableText}

{ingestionBatchingPolicyText}

{retentionPolicyText}

{mergePolicyText}";
            //{cachePolicyText}
            //{restrictedViewPolicyText}";

            await _databaseGateway.ExecuteCommandAsync(commandText, r => 0, ct);
        }

        private async Task EnsureAllQueuedAsync(
            TableDefinition stagingTable,
            IEnumerable<TransactionItem> adds,
            CancellationToken ct)
        {
            var toBeAdded = adds
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
            if (metadata.State == TransactionItemState.Done)
            {
                var schema = metadata.Schema!.Append(new ColumnDefinition
                {
                    ColumnName = BLOB_PATH_COLUMN,
                    ColumnType = "string"
                });
                var columnsText = schema
                    .Select(c => $"['{c.ColumnName}']:{c.ColumnType}");
                var schemaText = string.Join(", ", columnsText);
                var createTableText = $".create-merge table {_tableStatus.TableName} ({schemaText})";
                var newMetadata = metadata.UpdateState(TransactionItemState.Done);

                Trace.WriteLine(
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

        private string CreateStagingTableName(int startTxId)
        {
            var uniqueId = DateTime.UtcNow.Ticks.ToString("x8");

            return $"KM_Staging_{_tableStatus.TableName}_{startTxId}_{uniqueId}";
        }
    }
}