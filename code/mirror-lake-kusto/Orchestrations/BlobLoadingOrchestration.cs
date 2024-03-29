﻿using Kusto.Data.Common;
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
                    .Select(a => a.InternalState.Add!.StagingExtentId)
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
                    a => a.InternalState.Add!.StagingExtentId = null));

            return newAdds;
        }

        private async Task<IImmutableList<TransactionItem>> RemoveBlobPathsAsync(
            CancellationToken ct)
        {
            var toRemove = _logs.Removes
                .Where(r => r.State != TransactionItemState.Done);

            if (toRemove.Any())
            {
                Trace.WriteLine($"Removing {toRemove.Count()} blobs");

                var toRemoveIndex = toRemove.ToImmutableDictionary(r => r.BlobPath!, r => r);
                var historicalAdds = _tableStatus.GetHistorical(_logs.StartTxId).Adds;
                var matchHistory = historicalAdds
                    .Where(a => toRemoveIndex.ContainsKey(a.BlobPath!))
                    .Select(a => new
                    {
                        Add = a,
                        Remove = toRemoveIndex[a.BlobPath!],
                        ToDelete = a.State != TransactionItemState.Skipped
                    })
                    .ToImmutableArray();

                if (matchHistory.Length != toRemoveIndex.Count)
                {
                    throw new MirrorException(
                        "Couldn't find all past adds corresponding to removes:  "
                        + $"missing {toRemoveIndex.Count - matchHistory.Length} blobs");
                }

                var toDelete = matchHistory
                    .Where(m => m.ToDelete)
                    .Select(m => new
                    {
                        IngestionTime = m.Add.InternalState!.Add!.IngestionTime!,
                        BlobPath = m.Remove.BlobPath!
                    });

                if (toDelete.Any())
                {
                    var predicates = toDelete
                        .Select(d => $"({_stagingTable.BlobPathColumnName}=='{d.BlobPath}'"
                        + $" and ingestion_time()==datetime({d.IngestionTime}))");
                    var predicateText =
                        string.Join($"{Environment.NewLine}or ", predicates);
                    var commandText = @$".delete table {_tableStatus.TableName} records <|
{_tableStatus.TableName}
| where
{predicateText}";

                    await _databaseGateway.ExecuteCommandAsync(commandText, r => 0, ct);
                }

                var removed = matchHistory
                    .Where(m => m.ToDelete)
                    .Select(m => m.Remove.UpdateState(TransactionItemState.Done));
                var skipped = matchHistory
                    .Where(m => !m.ToDelete)
                    .Select(m => m.Remove.UpdateState(TransactionItemState.Skipped));

                return removed.Concat(skipped).ToImmutableArray();
            }
            else
            {
                return ImmutableArray<TransactionItem>.Empty;
            }
        }
        #endregion
    }
}