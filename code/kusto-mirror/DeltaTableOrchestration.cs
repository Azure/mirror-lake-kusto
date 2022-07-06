using Kusto.Mirror.ConsoleApp.Database;
using Kusto.Mirror.ConsoleApp.Storage;
using Kusto.Mirror.ConsoleApp.Storage.DeltaLake;
using System.Collections.Immutable;

namespace Kusto.Mirror.ConsoleApp
{
    internal class DeltaTableOrchestration
    {
        private readonly TableStatus _tableStatus;
        private readonly DeltaTableGateway _deltaTableGateway;

        public DeltaTableOrchestration(
            TableStatus tableStatus,
            DeltaTableGateway deltaTableGateway)
        {
            _tableStatus = tableStatus;
            _deltaTableGateway = deltaTableGateway;
        }

        public string KustoDatabaseName => _tableStatus.DatabaseName;
        
        public string KustoTableName => _tableStatus.TableName;

        internal async Task RunAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                if (_tableStatus.IsBatchIncomplete)
                {
                    throw new NotImplementedException();
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

        private async Task PersistNewBatchAsync(
            IImmutableList<TransactionLog> newLogs,
            CancellationToken ct)
        {
            var mergedLogs = TransactionLog.Coalesce(newLogs);

            await _tableStatus.PersistNewBatchAsync(mergedLogs, ct);
        }
    }
}