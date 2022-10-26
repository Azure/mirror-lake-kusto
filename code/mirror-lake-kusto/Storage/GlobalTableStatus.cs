using Azure.Core;
using Microsoft.Identity.Client.Platforms.Features.DesktopOs.Kerberos;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MirrorLakeKusto.Storage
{
    internal class GlobalTableStatus
    {
        private const string CHECKPOINT_BLOB = "index.csv";

        //  Items not belonging to any of the "planned" tables
        //  Typically those would be from a past run and we'll keep them around
        private readonly IImmutableList<TransactionItem> _orphanItems;
        private readonly IImmutableDictionary<string, TableStatus> _tableStatusIndex;
        private CheckpointGateway _checkpointGateway;

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
            var dedupItems = items
                .GroupBy(i => i.GetItemKey())
                .Select(g => g.Last());
            var itemByTableName = dedupItems
                .GroupBy(i => i.KustoTableName)
                .ToImmutableDictionary(g => g.Key);
            var noItems = ImmutableArray<TransactionItem>.Empty;

            _orphanItems = itemByTableName
                .Where(p => !tableNames.Contains(p.Key))
                .SelectMany(p => p.Value)
                .ToImmutableArray();
            _tableStatusIndex = tableNames
                .Select(t => new TableStatus(
                    this,
                    t,
                    itemByTableName.ContainsKey(t) ? itemByTableName[t] : noItems))
                .ToImmutableDictionary(s => s.TableName, s => s);

            _checkpointGateway = checkpointGateway;
        }
        #endregion

        public Uri CheckpointUri => _checkpointGateway.BlobUri;

        public TableStatus this[string table]
        {
            get
            {
                if (_tableStatusIndex.TryGetValue(table, out var tableStatus))
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

            if (_checkpointGateway.CanWrite)
            {
                await _checkpointGateway.WriteAsync(itemsContent, ct);
            }
        }

        private async Task CompactAsync(CancellationToken ct)
        {
            const long MAX_LENGTH = 4000000;

            var tempCheckpointGateway =
                await _checkpointGateway.GetTemporaryCheckpointGatewayAsync(ct);
            var allItems = _tableStatusIndex.Values
                .Select(s => s.AllStatus)
                .Prepend(_orphanItems)
                .SelectMany(s => s);

            using (var stream = new MemoryStream())
            {
                var headerBuffer = TransactionItem.HeaderToCsv();

                stream.Write(headerBuffer, 0, headerBuffer.Length);

                foreach (var item in allItems)
                {
                    var positionBefore = stream.Position;

                    item.ToCsv(stream);

                    var positionAfter = stream.Position;

                    if (positionAfter > MAX_LENGTH)
                    {
                        stream.SetLength(positionBefore);
                        stream.Flush();
                        await tempCheckpointGateway.WriteAsync(stream.GetBuffer(), ct);
                        stream.SetLength(0);
                        item.ToCsv(stream);
                    }
                }
                if (stream.Position > 0)
                {
                    stream.Flush();
                    await tempCheckpointGateway.WriteAsync(stream.GetBuffer(), ct);
                }
            }

            _checkpointGateway = tempCheckpointGateway;
        }
    }
}