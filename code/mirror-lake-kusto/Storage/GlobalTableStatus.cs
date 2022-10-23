using Azure.Core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MirrorLakeKusto.Storage
{
    internal class GlobalTableStatus
    {
        private const string CHECKPOINT_BLOB = "index.csv";
        private const string TEMP_CHECKPOINT_BLOB = "temp-index.csv";

        private readonly CheckpointGateway _checkpointGateway;
        //  Items not belonging to any of the "planned" tables
        //  Typically those would be from a past run and we'll keep them around
        private readonly IImmutableList<TransactionItem> _orphanItems;
        private readonly IImmutableDictionary<string, TableStatus> _tableStatusIndex;

        #region Constructors
        public static async Task<GlobalTableStatus> RetrieveAsync(
            Uri checkpointBlobFolderUrl,
            TokenCredential credential,
            IEnumerable<string> tableNames,
            CancellationToken ct)
        {
            var checkpointBlobUrl = new Uri(
                Path.Combine(checkpointBlobFolderUrl.ToString(), CHECKPOINT_BLOB));
            var checkpointGateway = new CheckpointGateway(checkpointBlobUrl, credential);

            if (!(await checkpointGateway.ExistsAsync(ct)))
            {
                await checkpointGateway.CreateAsync(ct);

                return new GlobalTableStatus(
                    checkpointGateway,
                    tableNames,
                    new TransactionItem[0]);
            }
            else
            {
                var buffer = await checkpointGateway.ReadAllContentAsync(ct);

                //  Rewrite the content in one clean append-blob
                //  Ensure it's an append blob + compact it
                Trace.TraceInformation("Rewrite checkpoint blob...");

                var items = TransactionItem.FromCsv(buffer, true);
                var globalTableStatus =
                    new GlobalTableStatus(checkpointGateway, tableNames, items);

                await globalTableStatus.CompactAsync(ct);

                return globalTableStatus;
            }
        }

        private GlobalTableStatus(
            CheckpointGateway checkpointGateway,
            IEnumerable<string> tableNames,
            IEnumerable<TransactionItem> items)
        {
            _checkpointGateway = checkpointGateway;

            var dedupItems = items
                .GroupBy(i => i.GetItemKey())
                .Select(g => g.Last());
            var itemByTableName = dedupItems
                .GroupBy(i => i.KustoTableName)
                .ToImmutableDictionary(g => g.Key);

            _orphanItems = itemByTableName
                .Where(p => !tableNames.Contains(p.Key))
                .SelectMany(p => p.Value)
                .ToImmutableArray();
            _tableStatusIndex = itemByTableName
                .Where(p => tableNames.Contains(p.Key))
                .Select(p => new TableStatus(this, p.Key, p.Value))
                .ToImmutableDictionary(s => s.TableName, s => s);
        }
        #endregion

        public TableStatus this[string table]
        {
            get
            {
                if(_tableStatusIndex.TryGetValue(table, out var tableStatus))
                {
                    return tableStatus;
                }
                else
                {
                    throw new InvalidOperationException(
                        $"Table '{table}' not found in global status");
                }
            }
        }

        internal async Task PersistNewItemsAsync(
            IEnumerable<TransactionItem> items,
            CancellationToken ct)
        {
            var itemsContent = TransactionItem.ToCsv(items);

            if (itemsContent.Length == 0)
            {
                throw new ArgumentException("Is empty", nameof(items));
            }

            await _checkpointGateway.WriteAsync(itemsContent, ct);
        }

        private Task CompactAsync(CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}