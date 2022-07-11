using Kusto.Cloud.Platform.Utils;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Kusto.Mirror.ConsoleApp.Storage
{
    internal class TableStatus
    {
        private readonly GlobalTableStatus _globalTableStatus;
        private IImmutableList<TransactionItem> _statuses;

        public TableStatus(
            GlobalTableStatus globalTableStatus,
            string databaseName,
            string tableName,
            IEnumerable<TransactionItem> blobStatuses)
        {
            _globalTableStatus = globalTableStatus;
            DatabaseName = databaseName;
            TableName = tableName;
            _statuses = blobStatuses.ToImmutableArray();
        }

        public string DatabaseName { get; }

        public string TableName { get; }

        public bool IsBatchIncomplete
        {
            get
            {
                var isBatchIncomplete = _statuses
                    .Where(s => IsComplete(s.State))
                    .Any();

                return isBatchIncomplete;
            }
        }

        public int? LastTxId
        {
            get
            {
                var lastTxId = _statuses.Any()
                    ? (int?)_statuses.Max(s => s.EndTxId)
                    : null;

                return lastTxId;
            }
        }

        public TransactionLog GetEarliestIncompleteBatch()
        {
            var startTxId = _statuses
                .Where(s => IsComplete(s.State))
                .Select(s => s.StartTxId)
                .First();

            return GetBatch(startTxId);
        }

        public TransactionLog GetBatch(long startTxId)
        {
            var batchItems = _statuses
                .Where(s => s.StartTxId == startTxId);

            return new TransactionLog(batchItems);
        }

        public TransactionLog Refresh(TransactionLog log)
        {
            var startTxId = log.StartTxId;
            var endTxId = log.EndTxId;
            var batchItems = _statuses
                .Where(s => s.StartTxId == startTxId);
            var logs = new TransactionLog(batchItems);

            foreach (var item in logs.AllItems)
            {
                if (item.EndTxId != endTxId)
                {
                    throw new InvalidOperationException("Invalid End transaction ID");
                }
            }

            return new TransactionLog(batchItems);
        }

        public async Task PersistNewItemsAsync(
            IEnumerable<TransactionItem> items,
            CancellationToken ct)
        {
            await _globalTableStatus.PersistNewItemsAsync(items, ct);

            //  Refresh the status
            var newStatus = _globalTableStatus.GetSingleTableStatus(DatabaseName, TableName);

            _statuses = newStatus._statuses;
        }

        private static bool IsComplete(TransactionItemState state)
        {
            return state != TransactionItemState.Done;
        }
    }
}