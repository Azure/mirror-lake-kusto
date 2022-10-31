using Kusto.Cloud.Platform.Utils;
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
        private static readonly TimeSpan BETWEEN_TX_PROBE_DELAY = TimeSpan.FromSeconds(5);

        private readonly TableStatus _tableStatus;
        private readonly DeltaTableGateway _deltaTableGateway;
        private readonly DatabaseGateway _databaseGateway;
        private readonly string? _creationTime;
        private readonly DateTime? _goBack;
        private readonly bool _continuousRun;
        private readonly bool _isFreeCluster;

        public DeltaTableOrchestration(
            string databaseName,
            TableStatus tableStatus,
            DeltaTableGateway deltaTableGateway,
            DatabaseGateway databaseGateway,
            string? creationTime,
            DateTime? goBack,
            bool continuousRun,
            bool isFreeCluster)
        {
            _tableStatus = tableStatus;
            _deltaTableGateway = deltaTableGateway;
            _databaseGateway = databaseGateway;
            _creationTime = creationTime;
            _goBack = goBack;
            _continuousRun = continuousRun;
            _isFreeCluster = isFreeCluster;
            KustoDatabaseName = databaseName;
        }

        public string KustoDatabaseName { get; }

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
                logs.StagingTable!.InternalState.StagingTable!.StagingTableName!;
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
                $"Elapsed time for transaction batch {logs.AllItems.First().StartTxId} "
                + $"to {logs.AllItems.First().EndTxId}:  {stopwatch.Elapsed}");
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
            await BlobAnalysisOrchestration.EnsureAllAnalyzedAsync(
                _databaseGateway,
                stagingTable,
                _tableStatus,
                logs.StartTxId,
                _creationTime,
                _goBack,
                ct);
            await BlobStagingOrchestration.EnsureAllStagedAsync(
                _databaseGateway,
                stagingTable,
                _tableStatus,
                logs.StartTxId,
                ct);
            if (logs.Metadata != null && logs.StartTxId != 0)
            {
                await EnsureLandingTableSchemaAsync(stagingTable, logs.Metadata, ct);
            }
            await BlobLoadingOrchestration.EnsureAllLoadedAsync(
                _databaseGateway,
                stagingTable,
                _tableStatus,
                logs.StartTxId,
                ct);
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
                .Clone(s => s.InternalState.StagingTable!.StagingTableName = stagingTableName);
            var resetAdds = log.Adds
                .Where(a => a.State != TransactionItemState.Initial)
                .Select(a => a.UpdateState(TransactionItemState.Initial));
            var template = log.AllItems.First();

            Trace.WriteLine(
                $"Resetting transaction batch {template.StartTxId} to {template.EndTxId}");

            await _tableStatus.PersistNewItemsAsync(resetAdds.Append(stagingTable), ct);
            await ProcessTransactionBatchAsync(startTxId, ct);
        }

        private async Task DropStagingTableAsync(
            TransactionItem stagingTable,
            CancellationToken ct)
        {
            var stagingTableName =
                stagingTable.InternalState.StagingTable!.StagingTableName;
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
  ""SoftDeletePeriod"": ""40000.00:00:00""
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
                //  Don't delete anything in the table
                var retentionPolicyText =
                    _isFreeCluster && _goBack != null
                    ? string.Empty
                    : @$".alter table {_tableStatus.TableName} policy retention 
```
{{
  ""SoftDeletePeriod"": ""{(int)DateTime.Now.Subtract(_goBack!.Value).Days}.00:00:00""
}}
```";
                var commandText = @$"
.execute database script with (ContinueOnErrors=false, ThrowOnErrors=true) <|
{createTableText}

{retentionPolicyText}
";

                Trace.TraceInformation(
                    "Updating schema of Kusto table "
                    + $"'{KustoDatabaseName}.{_tableStatus.TableName}'");

                await _databaseGateway.ExecuteCommandAsync(commandText, r => 0, ct);
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

            return $"MLK_Staging_{_tableStatus.TableName}_{startTxId}_{uniqueId}";
        }
    }
}