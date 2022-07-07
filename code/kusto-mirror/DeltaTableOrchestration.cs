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

            throw new NotImplementedException();
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