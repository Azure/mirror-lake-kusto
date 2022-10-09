﻿using Kusto.Cloud.Platform.Utils;
using Kusto.Data.Common;
using Kusto.Data.Exceptions;
using Kusto.Data.Ingestion;
using Kusto.Ingest;
using MirrorLakeKusto.Kusto;
using MirrorLakeKusto.Storage;
using MirrorLakeKusto.Storage.DeltaLake;
using System.Collections.Immutable;
using System.Diagnostics;

namespace MirrorLakeKusto.Orchestrations
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
                    var currentLog = _tableStatus.GetAllDoneLogs();
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
            var stopwatch = new Stopwatch();
            var logs = _tableStatus.GetBatch(startTxId);

            stopwatch.Start();
            Trace.WriteLine(
                $"Processing Transaction Batch {logs.AllItems.First().StartTxId} "
                + $"to {logs.AllItems.First().EndTxId}");

            var mainTable = _tableStatus
                .GetTableDefinition(startTxId)
                .WithTrackingColumns();
            var stagingTableName =
                logs.StagingTable!.InternalState.StagingTableInternalState!.StagingTableName!;
            var stagingTable = mainTable.RenameTable(stagingTableName);
            var isStaging = await EnsureStagingTableAsync(
                stagingTable,
                logs.StagingTable!,
                ct);

            if (!isStaging)
            {
                var notDone = logs.AllItems
                    .Where(l => l.State != TransactionItemState.Done);

                if (notDone.Count() == 1)
                {
                    if (notDone.First().Action != TransactionItemAction.StagingTable)
                    {
                        throw new MirrorException(
                            "Should only have the staging table remain, instead:  "
                            + $"{notDone.First().Action} / {notDone.First().State}");
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
            Trace.WriteLine(
                $"Elapsed time for transaction batch {logs.AllItems.First().StartTxId}"
                + "to {logs.AllItems.First().EndTxId}:  {stopwatch.Elapsed}");
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
            }
            await BlobStagingOrchestration.EnsureAllStagedAsync(
                _databaseGateway,
                stagingTable,
                _tableStatus,
                logs.StartTxId,
                _deltaTableGateway.DeltaTableStorageUrl,
                ct);
            await EnsureAllLoadedAsync(mainTable, stagingTable, logs.StartTxId, ct);
            await DropStagingTableAsync(logs.StagingTable!, ct);
            Trace.TraceInformation(
                $"Table '{_tableStatus.TableName}', "
                + $"tx {logs.StartTxId} to {logs.EndTxId} processed");
        }

        private async Task ResetTransactionBatchAsync(
            long startTxId,
            CancellationToken ct)
        {
            var log = _tableStatus.GetBatch(startTxId);
            var stagingTableName = CreateStagingTableName(log.StartTxId);
            var stagingTable = log.StagingTable!
                .UpdateState(TransactionItemState.Initial)
                .Clone(s => s.InternalState.StagingTableInternalState!.StagingTableName = stagingTableName);
            var resetAdds = log.Adds
                .Where(a => a.State != TransactionItemState.Initial)
                .Select(a => a.UpdateState(TransactionItemState.Initial));
            var template = log.AllItems.First();

            Trace.WriteLine(
                $"Resetting transaction batch {template.StartTxId} to {template.EndTxId}");

            await _tableStatus.PersistNewItemsAsync(resetAdds.Append(stagingTable), ct);
            await ProcessTransactionBatchAsync(startTxId, ct);
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

            Trace.WriteLine($"Loading extents in main table");
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
            IEnumerable<Uri> blobPathToRemove,
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
            CancellationToken ct)
        {
            var stagingTableName =
                stagingTable.InternalState.StagingTableInternalState!.StagingTableName;
            var commandText = $".drop table {stagingTableName} ifexists";

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
                var counts = await _databaseGateway.ExecuteCommandAsync(
                    $".show tables | where TableName == '{stagingTableSchema.Name}' | count",
                    r => (long)r[0], ct);

                return counts.First() == 1;
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
            var batchingPolicyText = @$".alter table {stagingTableSchema.Name} policy ingestionbatching
```
{{
    ""MaximumBatchingTimeSpan"" : ""00:00:10""
}}
```
";
            var nonBlobPathColumnsMappingText = stagingTableSchema.Columns
                .Where(c => c.ColumnName != stagingTableSchema.BlobPathColumnName)
                .Select(c => @$"{{""Column"": ""{c.ColumnName}"", "
                + @$"""Properties"": {{""Path"": ""$.{c.ColumnName}""}} }}");
            var mappingText = $@"[
    {string.Join(", ", nonBlobPathColumnsMappingText)},
    {{
        ""Column"": ""{stagingTableSchema.BlobPathColumnName}"",
        ""Properties"":
        {{
            ""Path"": ""$.{stagingTableSchema.BlobPathColumnName}"",
            ""Transform"": ""SourceLocation""
        }}
    }}
]
";
            var ingestionMappingText = $@"
.create table {stagingTableSchema.Name} ingestion parquet mapping ""Mapping""
```
{mappingText}
```
";
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
                : string.Empty;// $".alter table {stagingTableSchema.Name} policy restricted_view_access true";
            var commandText = @$"
.execute database script with (ContinueOnErrors=false, ThrowOnErrors=true) <|
{createTableText}

{batchingPolicyText}

{ingestionMappingText}

{retentionPolicyText}

{mergePolicyText}

{cachePolicyText}

{restrictedViewPolicyText}";

            await _databaseGateway.ExecuteCommandAsync(commandText, r => 0, ct);
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
                new StagingTableInternalState { StagingTableName = stagingTableName });
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