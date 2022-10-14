using Kusto.Cloud.Platform.Utils;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace MirrorLakeKusto.Storage
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
                    .Where(s => !IsComplete(s.State))
                    .Any();

                return isBatchIncomplete;
            }
        }

        public long GetEarliestIncompleteBatchTxId()
        {
            var startTxId = _statuses
                .Where(s => !IsComplete(s.State))
                .Select(s => s.StartTxId)
                .First();

            return startTxId;
        }

        public TransactionLog? GetAllDoneLogs()
        {
            if (_statuses.Any())
            {
                var logs = _statuses
                    .Where(s => s.Action != TransactionItemAction.StagingTable)
                    .GroupBy(s => s.StartTxId)
                    .Select(g => new TransactionLog(g));
                var all = TransactionLog.Coalesce(logs);

                return all;
            }
            else
            {
                return null;
            }
        }

        public TransactionLog GetBatch(long startTxId)
        {
            var batchItems = _statuses
                .Where(s => s.StartTxId == startTxId);

            return new TransactionLog(batchItems);
        }

        public TransactionLog GetHistorical(long beforeTxId)
        {
            var logs = _statuses
                .Where(s => s.EndTxId < beforeTxId)
                .GroupBy(s => s.StartTxId)
                .Select(g => new TransactionLog(g));
            var log = TransactionLog.Coalesce(logs);

            return log;
        }

        public TableDefinition GetTableDefinition(long upToTxId)
        {
            var schemaItem = _statuses
                .Where(s => s.StartTxId <= upToTxId)
                .Where(s => s.Action == TransactionItemAction.Schema)
                .OrderByDescending(s => s.StartTxId)
                .FirstOrDefault();

            if (schemaItem == null)
            {
                throw new MirrorException("No schema defined in transactions");
            }
            if (schemaItem.Schema == null || schemaItem.PartitionColumns == null)
            {
                throw new MirrorException("No schema or partition columns in the schema item");
            }

            return new TableDefinition(TableName, schemaItem.Schema, schemaItem.PartitionColumns);
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
            return state == TransactionItemState.Done;
        }
    }
}