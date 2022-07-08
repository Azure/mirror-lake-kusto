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

            if (log.AllItems.First().StartTxId == 0)
            {
                if (log.Metadata == null)
                {
                    throw new InvalidOperationException("Transaction 0 should have meta data");
                }
                await EnsureTableSchemaAsync(log.Metadata, ct);
            }
            var stagingTable = await EnsureStagingTableAsync(
                log.AllItems.First().StartTxId,
                log.Metadata?.Schema,
                ct);

            await EnsureAllQueuedAsync(log.Adds, ct);
        }

        private Task EnsureAllQueuedAsync(
            IEnumerable<TransactionItem> adds,
            CancellationToken ct)
        {
            throw new NotImplementedException();
        }

        private async Task<string> EnsureStagingTableAsync(
            int startTxId,
            IImmutableList<ColumnDefinition>? schema,
            CancellationToken ct)
        {
            var stagingTableName = $"KM_Staging_{_tableStatus.TableName}_{startTxId}";
            var schemaText = await GetSchemaTextForStagingTableAsync(schema, ct);
            var createTableText = $".create-merge table {stagingTableName} ({schemaText})";
            //  Disable merge policy not to run after phantom extents
            var mergePolicyText = @$".alter table {stagingTableName} policy merge
```
{{
  ""AllowRebuild"": false,
  ""AllowMerge"": false
}}
```";
            //  Don't cache anything not to potentially overload the cache with big historical data
            var cachePolicyText = $".alter table {stagingTableName} policy caching hot = 0d";
            //  Don't delete anything in the table
            var retentionPolicyText = @$".alter table {stagingTableName} policy retention 
```
{{
  ""SoftDeletePeriod"": ""10000000:0:0:0""
}}
```";
            //  Staging table:  shouldn't be queried by normal users
            var restrictedViewPolicyText =
                $".alter table {stagingTableName} policy restricted_view_access true";
            var ingestionBatchingPolicyText = @$".alter table {stagingTableName} policy ingestionbatching
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

            return stagingTableName;
        }

        private async Task<string> GetSchemaTextForStagingTableAsync(
            IImmutableList<ColumnDefinition>? schema,
            CancellationToken ct)
        {
            if (schema != null)
            {
                var realSchema = schema.Append(new ColumnDefinition
                {
                    ColumnName = BLOB_PATH_COLUMN,
                    ColumnType = "string"
                }).Append(new ColumnDefinition
                {
                    ColumnName = BLOB_ROW_NUMBER_COLUMN,
                    ColumnType = "long"
                });
                var columnsText = schema
                    .Select(c => $"['{c.ColumnName}']:{c.ColumnType}");
                var schemaText = string.Join(", ", columnsText);

                return schemaText;
            }
            else
            {
                var tableSchema = await _databaseGateway.ExecuteCommandAsync(
                    $".show table {_tableStatus.TableName} schema as csl",
                    r => r["Schema"],
                    ct);
                var stagingTableSchema = $"{tableSchema},{BLOB_ROW_NUMBER_COLUMN}:long";

                return stagingTableSchema;
            }
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