using Kusto.Cloud.Platform.Utils;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Kusto.Mirror.ConsoleApp.Database
{
    internal class TableStatus
    {
        private const string STATUS_TABLE_NAME = "KM_DeltaStatus";
        private readonly DatabaseGateway _database;
        private IImmutableList<TransactionItem> _statuses;

        public static async Task<IImmutableList<TableStatus>> LoadStatusTableAsync(
            DatabaseGateway database,
            IEnumerable<string> tables,
            CancellationToken ct)
        {
            var tableNameList = string.Join(", ", tables.Select(t => $"\"{t}\""));
            var query = $@"{STATUS_TABLE_NAME}
| extend IngestionTime=ingestion_time()
| where KustoTableName in ({tableNameList})";
            var blobStatuses = await database.ExecuteQueryAsync(
                query,
                r => DeserializeBlobStatus(r),
                ct);
            var redundantStatuses = blobStatuses
                .GroupBy(b => $"{b.KustoTableName}-{b.StartTxId}-{b.BlobPath}")
                .Where(g => g.Count() > 1)
                .Select(g => g.Take(g.Count() - 1))
                .SelectMany(b => b)
                .ToImmutableArray();

            if (redundantStatuses.Any())
            {
                throw new NotImplementedException("Corrupted status table");
            }
            else
            {
                var tableStatuses = blobStatuses
                    .GroupBy(b => b.KustoTableName)
                    .ToImmutableDictionary(g => g.Key);
                var tableStatusCollection = tables
                    .Select(t => tableStatuses.ContainsKey(t)
                    ? new TableStatus(database, t, tableStatuses[t])
                    : new TableStatus(database, t, new TransactionItem[0]))
                    .ToImmutableArray();

                return tableStatusCollection;
            }
        }

        public async Task PersistNewBatchAsync(TransactionLog log, CancellationToken ct)
        {
            await IngestTransactionItemsAsync(log.AllItems, ct);
        }

        private async Task IngestTransactionItemsAsync(
            IEnumerable<TransactionItem> allItems,
            CancellationToken ct)
        {
            var itemsJson = JsonSerializer.Serialize(allItems);
            var commandText = "";

            await _database.ExecuteCommandAsync(
                commandText,
                p => 0,
                ct);
        }

        private TableStatus(
            DatabaseGateway database,
            string tableName,
            IEnumerable<TransactionItem> blobStatuses)
        {
            _database = database;
            TableName = tableName;
            _statuses = blobStatuses.ToImmutableArray();
        }

        public string TableName { get; }

        internal static string CreateStatusTableCommandText =>
            $".create-merge table {STATUS_TABLE_NAME}(KustoTableName:string, "
            + "StartTxId:int, EndTxId:int, "
            + "Action:string, State:string, Timestamp:datetime, "
            + "BlobPath:string, PartitionValues:dynamic, "
            + "Size:long, RecordCount:long, "
            + "DeltaTableId:string, DeltaTableName:string, "
            + "PartitionColumns:dynamic, Schema:dynamic)";

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