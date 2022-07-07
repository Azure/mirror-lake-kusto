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
        private const string STATUS_TABLE_NAME = "KM_DeltaStatus";
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

        public async Task PersistNewItemsAsync(
            IEnumerable<TransactionItem> items,
            CancellationToken ct)
        {
            await _globalTableStatus.PersistNewItemsAsync(items, ct);
            
            //  Refresh the status
            var newStatus = _globalTableStatus.GetSingleTableStatus(DatabaseName, TableName);

            _statuses = newStatus._statuses;
        }

        public bool IsBatchIncomplete
        {
            get
            {
                var isBatchIncomplete = _statuses
                    .Where(s => s.State != TransactionItemState.Loaded
                    || s.State != TransactionItemState.Deleted
                    || s.State != TransactionItemState.Applied)
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
    }
}