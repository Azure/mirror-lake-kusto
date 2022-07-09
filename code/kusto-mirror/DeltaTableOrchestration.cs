using Kusto.Cloud.Platform.Utils;
using Kusto.Data.Common;
using Kusto.Data.Ingestion;
using Kusto.Ingest;
using Kusto.Mirror.ConsoleApp.Database;
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
                    await ProcessBatchAsync(ct);
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
                        await PersistNewBatchAsync(newLogs, ct);
                    }
                    else
                    {
                        throw new NotImplementedException();
                    }
                }
            }
        }

        private async Task ProcessBatchAsync(CancellationToken ct)
        {
            var log = _tableStatus.GetEarliestIncompleteBatch();
            var startTxId = log.AllItems.First().StartTxId;
            var stagingTableName = $"KM_Staging_{_tableStatus.TableName}_{startTxId}";

            if (startTxId == 0)
            {
                if (log.Metadata == null)
                {
                    throw new InvalidOperationException("Transaction 0 should have meta data");
                }
                await EnsureTableSchemaAsync(log.Metadata, ct);
            }
            var targetTable = await GetTableSchemaAsync(log.Metadata, ct);
            var stagingTable = GetStagingTableSchema(stagingTableName, targetTable);

            await EnsureStagingTableAsync(stagingTable, ct);
            await EnsureAllQueuedAsync(stagingTable, log.Adds, ct);

            throw new NotImplementedException();
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

        private async Task EnsureStagingTableAsync(
            TableDefinition stagingTable,
            CancellationToken ct)
        {
            var schemaText = string.Join(
                ", ",
                stagingTable.Columns.Select(c => $"['{c.ColumnName}']:{c.ColumnType}"));
            var createTableText = $".create-merge table {stagingTable.Name} ({schemaText})";
            //  Disable merge policy not to run after phantom extents
            var mergePolicyText = @$".alter table {stagingTable.Name} policy merge
```
{{
  ""AllowRebuild"": false,
  ""AllowMerge"": false
}}
```";
            //  Don't cache anything not to potentially overload the cache with big historical data
            var cachePolicyText = $".alter table {stagingTable.Name} policy caching hot = 0d";
            //  Don't delete anything in the table
            var retentionPolicyText = @$".alter table {stagingTable.Name} policy retention 
```
{{
  ""SoftDeletePeriod"": ""10000000:0:0:0""
}}
```";
            //  Staging table:  shouldn't be queried by normal users
            var restrictedViewPolicyText =
                $".alter table {stagingTable.Name} policy restricted_view_access true";
            var ingestionBatchingPolicyText = @$".alter table {stagingTable.Name} policy ingestionbatching
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
                .Where(i => i.State == TransactionItemState.ToBeAdded);

            if (toBeAdded.Any())
            {
                var ingestionMappings = CreateIngestionMappings(stagingTable);
                var queueTasks = toBeAdded
                    .Select(async item =>
                    {
                        await QueueItemAsync(item, stagingTable, ingestionMappings, ct);
                    })
                    .ToImmutableArray();
                var newItems = toBeAdded
                    .Select(item => item.UpdateState(TransactionItemState.QueuedForIngestion));

                await Task.WhenAll(queueTasks);
                await _tableStatus.PersistNewItemsAsync(newItems, ct);
            }
        }

        private async Task QueueItemAsync(
            TransactionItem item,
            TableDefinition stagingTable,
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

            await _databaseGateway.QueueIngestionAsync(
                new Uri(fullBlobath),
                stagingTable.Name,
                DataSourceFormat.parquet,
                ingestionMappings,
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
            if (metadata.State == TransactionItemState.ToBeApplied)
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
                var newMetadata = metadata.UpdateState(TransactionItemState.Applied);

                Trace.WriteLine(
                    "Updating schema of Kusto table "
                    + $"'{_tableStatus.DatabaseName}.{_tableStatus.TableName}'");

                await _databaseGateway.ExecuteCommandAsync(createTableText, r => 0, ct);
                await _tableStatus.PersistNewItemsAsync(new[] { newMetadata }, ct);
            }
        }

        private async Task PersistNewBatchAsync(
            IImmutableList<TransactionLog> newLogs,
            CancellationToken ct)
        {
            var mergedLogs = TransactionLog.Coalesce(newLogs);

            await _tableStatus.PersistNewItemsAsync(mergedLogs.AllItems, ct);
        }
    }
}