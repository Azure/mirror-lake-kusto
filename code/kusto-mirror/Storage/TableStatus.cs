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

        public Task PersistNewBatchAsync(TransactionLog log, CancellationToken ct)
        {
            throw new NotImplementedException();
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

        private static TransactionItem DeserializeBlobStatus(IDataRecord r)
        {
            var actionText = (string)r["Action"];
            var action = Enum.Parse<TransactionItemAction>(actionText);

            switch (action)
            {
                case TransactionItemAction.Add:
                    return TransactionItem.CreateAddItem(
                        (string)r["KustoTableName"],
                        (int)r["StartTxId"],
                        (int)r["EndTxId"],
                        Enum.Parse<TransactionItemState>((string)r["State"]),
                        (DateTime)r["Timestamp"],
                        (DateTime?)r["IngestionTime"],
                        (string)r["BlobPath"],
                        DeserializeDictionary((string)r["PartitionValues"]),
                        (long)r["Size"],
                        (long)r["RecordCount"]);
                case TransactionItemAction.Remove:
                    return TransactionItem.CreateRemoveItem(
                        (string)r["KustoTableName"],
                        (int)r["StartTxId"],
                        (int)r["EndTxId"],
                        Enum.Parse<TransactionItemState>((string)r["State"]),
                        (DateTime)r["Timestamp"],
                        (DateTime?)r["IngestionTime"],
                        (string)r["BlobPath"],
                        DeserializeDictionary((string)r["PartitionValues"]),
                        (long)r["Size"]);
                case TransactionItemAction.Schema:
                    return TransactionItem.CreateSchemaItem(
                        (string)r["KustoTableName"],
                        (int)r["StartTxId"],
                        (int)r["EndTxId"],
                        Enum.Parse<TransactionItemState>((string)r["State"]),
                        (DateTime)r["Timestamp"],
                        (DateTime?)r["IngestionTime"],
                        Guid.Parse((string)r["DeltaTableId"]),
                        (string)r["DeltaTableName"],
                        DeserializeArray((string)r["PartitionColumns"]),
                        DeserializeDictionary((string)r["Schema"]));
                default:
                    throw new NotSupportedException($"Action '{action}'");
            }
        }

        private static IImmutableList<string> DeserializeArray(string text)
        {
            var array = JsonSerializer.Deserialize<ImmutableArray<string>>(text);

            if (array == null)
            {
                throw new MirrorException($"Can't deserialize list:  '{text}'");
            }

            return array;
        }

        private static IImmutableDictionary<string, string> DeserializeDictionary(string text)
        {
            var map = JsonSerializer.Deserialize<ImmutableDictionary<string, string>>(text);

            if (map == null)
            {
                throw new MirrorException($"Can't deserialize dictionary:  '{text}'");
            }

            return map;
        }
    }
}