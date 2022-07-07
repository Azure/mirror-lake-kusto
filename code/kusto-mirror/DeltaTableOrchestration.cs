using Kusto.Mirror.ConsoleApp.Database;
using Kusto.Mirror.ConsoleApp.Storage;
using Kusto.Mirror.ConsoleApp.Storage.DeltaLake;
using System.Collections.Immutable;
using System.Diagnostics;

namespace Kusto.Mirror.ConsoleApp
{
    internal class DeltaTableOrchestration
    {
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
                if (log.Metadata.State != TransactionItemState.Applied)
                {
                    await EnsureTableSchemaAsync(log.Metadata, ct);
                }
            }

            throw new NotImplementedException();
        }

        private async Task EnsureTableSchemaAsync(TransactionItem metadata, CancellationToken ct)
        {
            //var schemaText = metadata.Schema!
            //    .
            var createTableText = $".create-merge table {_tableStatus.TableName} ()";

            Trace.WriteLine(
                "Updating schema of Kusto table "
                + $"'{_tableStatus.DatabaseName}.{_tableStatus.TableName}'");

            await _databaseGateway.ExecuteCommandAsync(createTableText, r => 0, ct);

            throw new NotImplementedException();
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