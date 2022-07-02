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
        private readonly DatabaseGateway _database;

        public static async Task<IImmutableList<TableStatus>> LoadStatusTableAsync(
            DatabaseGateway database,
            IEnumerable<string> tables,
            CancellationToken ct)
        {
            var tableNameList = string.Join(", ", tables.Select(t => $"\"{t}\""));
            var query = $@"{StatusTableName}
| extend IngestionTime=ingestion_time()
| where TableName in ({tableNameList})";
            var blobStatuses = await database.ExecuteQueryAsync(
                query,
                r => DeserializeBlobStatus(r),
                ct);
            var redundantStatuses = blobStatuses
                .GroupBy(b => $"{b.TableName}-{b.TxId}-{b.BlobPath}")
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
        }

        public static string StatusTableName => "KM_DeltaStatus";

        public string TableName { get; }

        private static BlobStatus DeserializeBlobStatus(IDataRecord r)
        {
            return new BlobStatus(
                (string)r["TableName"],
                (int)r["TxId"],
                (string)r["BlobPath"],
                (string)r["Action"],
                (string)r["Status"],
                (DateTime)r["IngestionTime"]);
        }
    }
}