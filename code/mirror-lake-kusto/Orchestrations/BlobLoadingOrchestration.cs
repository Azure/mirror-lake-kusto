using Kusto.Data.Common;
using MirrorLakeKusto.Kusto;
using MirrorLakeKusto.Storage;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;

namespace MirrorLakeKusto.Orchestrations
{
    internal class BlobLoadingOrchestration
    {
        private readonly DatabaseGateway _databaseGateway;
        private readonly TableDefinition _stagingTable;
        private readonly TableStatus _tableStatus;
        private readonly TransactionLog _logs;

        #region Constructors
        public static async Task EnsureAllLoadedAsync(
            DatabaseGateway databaseGateway,
            TableDefinition stagingTable,
            TableStatus tableStatus,
            long startTxId,
            CancellationToken ct)
        {
            var orchestration = new BlobLoadingOrchestration(
                databaseGateway,
                stagingTable,
                tableStatus,
                startTxId);

            await orchestration.RunAsync(ct);
        }

        private BlobLoadingOrchestration(
            DatabaseGateway databaseGateway,
            TableDefinition stagingTable,
            TableStatus tableStatus,
            long startTxId)
        {
            _databaseGateway = databaseGateway;
            _stagingTable = stagingTable;
            _tableStatus = tableStatus;
            _logs = tableStatus.GetBatch(startTxId);
        }
        #endregion

        #region Orchestration
        private async Task RunAsync(CancellationToken ct)
        {
            var newAdds = await LoadExtentsAsync(ct);
            var newRemoves = await RemoveBlobPathsAsync(ct);
            var newItems = newAdds.Concat(newRemoves);

            await _tableStatus.PersistNewItemsAsync(newItems, ct);
        }

        private async Task<IEnumerable<TransactionItem>> LoadExtentsAsync(CancellationToken ct)
        {
            var toAdd = _logs.Adds
                .Where(a => a.State != TransactionItemState.Done
                && a.State != TransactionItemState.Skipped);

            if (toAdd.Any())
            {
                Trace.WriteLine($"Loading {toAdd.Count()} blobs");

                var extentIds = toAdd
                    .Select(a => a.InternalState.AddInternalState!.StagingExtentId)
                    .Distinct()
                    .ToImmutableArray();
                var extentIdsText = string.Join(", ", extentIds.Select(e => $"'{e}'"));
                var moveCommandText = $@".move extents
({extentIdsText})
from table {_stagingTable.Name}
to table {_tableStatus.TableName}";
                var results =
                    await _databaseGateway.ExecuteCommandAsync(moveCommandText, r => 0, ct);
            }

            var newAdds = toAdd
                .Select(a => a.UpdateState(TransactionItemState.Done))
                .Select(a => a.Clone(
                    a => a.InternalState.AddInternalState!.StagingExtentId = null));

            return newAdds;
        }

        private async Task<IEnumerable<TransactionItem>> RemoveBlobPathsAsync(CancellationToken ct)
        {
            var toRemove = _logs.Removes
                .Where(r => r.State != TransactionItemState.Done);

            if (toRemove.Any())
            {
                Trace.WriteLine($"Removing {toRemove.Count()} blobs");

                var blobPathsToRemove = toRemove.Select(b => b.BlobPath).ToHashSet();
                var historicalAdds = _tableStatus.GetHistorical(_logs.StartTxId).Adds;
                var data = historicalAdds
                    .Zip(Enumerable.Range(0, historicalAdds.Count), (add, index) => new { add, index })
                    .Where(a => blobPathsToRemove.Contains(a.add.BlobPath))
                    .Select(a => new
                    {
                        ParameterName = $"IngestionTime{a.index}",
                        BlobPath = a.add.BlobPath,
                        IngestionTime = a.add.InternalState.AddInternalState!.IngestionTime
                    })
                    .ToImmutableArray();

                Debug.Assert(
                    data.Length == blobPathsToRemove.Count,
                    "Couldn't find all past adds corresponding to removes");

                var blobQueriesListText = data
                    .Select(d => $"{_tableStatus.TableName} "
                    + $"| where {_stagingTable.BlobPathColumnName}=='{d.BlobPath}'"
                    + $" and ingestion_time()==datetime({d.IngestionTime})");
                var blobQueryText =
                    string.Join($"{Environment.NewLine}union ", blobQueriesListText);
                var commandText = @$".delete table {_tableStatus.TableName} records <|
{blobQueryText}";

                await _databaseGateway.ExecuteCommandAsync(commandText, r => 0, ct);
            }

            var newRemoves = toRemove
                .Select(a => a.UpdateState(TransactionItemState.Done));

            return newRemoves;
        }
        #endregion
    }
}