using Kusto.Cloud.Platform.Utils;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kusto.Mirror.ConsoleApp.Database
{
    internal class TableStatus
    {
        private const string STATUS_TABLE_NAME = "KM_DeltaStatus";
        private readonly DatabaseGateway _database;
        private IImmutableList<BlobStatus> _statuses;

        public static async Task<IImmutableList<TableStatus>> LoadStatusTableAsync(
            DatabaseGateway database,
            IEnumerable<string> tables,
            CancellationToken ct)
        {
            var tableNameList = string.Join(", ", tables.Select(t => $"\"{t}\""));
            var query = $@"{STATUS_TABLE_NAME}
| extend IngestionTime=ingestion_time()
| where TableName in ({tableNameList})";
            var blobStatuses = await database.ExecuteQueryAsync(
                query,
                r => DeserializeBlobStatus(r),
                ct);
            var redundantStatuses = blobStatuses
                .GroupBy(b => $"{b.TableName}-{b.StartTxId}-{b.BlobPath}")
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
                    .GroupBy(b => b.TableName)
                    .ToImmutableDictionary(g => g.Key);
                var tableStatusCollection = tables
                    .Select(t => tableStatuses.ContainsKey(t)
                    ? new TableStatus(database, t, tableStatuses[t])
                    : new TableStatus(database, t, new BlobStatus[0]))
                    .ToImmutableArray();

                return tableStatusCollection;
            }
        }

        private TableStatus(
            DatabaseGateway database,
            string tableName,
            IEnumerable<BlobStatus> blobStatuses)
        {
            _database = database;
            TableName = tableName;
            _statuses = blobStatuses.ToImmutableArray();
        }

        public string TableName { get; }

        internal static string CreateStatusTableCommandText =>
            $".create-merge table {STATUS_TABLE_NAME}(TableName:string, "
            + "StartTxId:int, EndTxId:int, "
            + "BlobPath:string, Action:string, Status:string)";

        public bool IsBatchIncomplete
        {
            get
            {
                var isBatchIncomplete = _statuses
                    .Where(s => s.Status != BlobState.Loaded.ToString()
                    || s.Status != BlobState.Deleted.ToString())
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

        private static BlobStatus DeserializeBlobStatus(IDataRecord r)
        {
            return new BlobStatus(
                (string)r["TableName"],
                (int)r["StartTxId"],
                (int)r["EndTxId"],
                (string)r["BlobPath"],
                (string)r["Action"],
                (string)r["Status"],
                (DateTime)r["IngestionTime"]);
        }
    }
}