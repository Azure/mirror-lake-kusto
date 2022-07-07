using Azure.Core;
using Kusto.Mirror.ConsoleApp.Storage.Bookmark;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kusto.Mirror.ConsoleApp.Storage
{
    internal class GlobalTableStatus
    {
        #region Inner Types
        private record TableKey(string Database, string Table);

        private record TransactionItemKey(
            int StartTxId,
            int EndTxId,
            string? BlobPath);

        private class TableItemIndex
        {
            public TableItemIndex(int initialBlockId, IEnumerable<TransactionItem> initialItems)
            {
                var pairs = initialItems
                    .Select(i => KeyValuePair.Create(
                        new TransactionItemKey(i.StartTxId, i.EndTxId, i.BlobPath),
                        i));

                BlockIds = new List<int>(new[] { initialBlockId });
                ItemIndex = new Dictionary<TransactionItemKey, TransactionItem>(pairs);
            }

            /// <summary>All block IDs used to persist blocks for that table.</summary>
            public IList<int> BlockIds { get; }

            /// <summary>In memory we only keep the latest for each item.</summary>
            public IDictionary<TransactionItemKey, TransactionItem> ItemIndex { get; }
        }
        #endregion

        private readonly BookmarkGateway _bookmarkGateway;
        //  It is assumed that tables can be accessed by different threads
        //  But that each table by itself is accessed a single-thread at the time
        private readonly ConcurrentDictionary<TableKey, TableItemIndex> _tableIndex;

        #region Constructors
        public static async Task<GlobalTableStatus> RetrieveAsync(
            Uri checkpointBlobUrl,
            TokenCredential credential,
            CancellationToken ct)
        {
            var bookmarkGateway = await BookmarkGateway.CreateAsync(
                checkpointBlobUrl,
                credential,
                false,
                ct);
            var blocks = await bookmarkGateway.ReadAllBlocksAsync(ct);

            if (blocks.Count() > 0)
            {   //  Rewrite the content in blocks in case somebody edited the file
                Trace.WriteLine("Rewrite checkpoint blob...");

                var content = await bookmarkGateway.ReadAllContentAsync(ct);
                var items = TransactionItem.FromCsv(content, true);
                var logList = items
                    .GroupBy(i => new TableKey(i.KustoDatabaseName, i.KustoTableName))
                    .Select(g => new
                    {
                        Content = TransactionItem.ToCsv(g),
                        //  De-duplicate items
                        Items = DeduplicateTransactionItems(g)
                    })
                    .ToImmutableList();
                var bookmarkTx = new BookmarkTransaction(
                    logList.Select(l => l.Content).Prepend(TransactionItem.GetCsvHeader()),
                    null,
                    blocks.Select(b => b.Id));
                var result = await bookmarkGateway.ApplyTransactionAsync(bookmarkTx, ct);

                return new GlobalTableStatus(
                    bookmarkGateway,
                    result.AddedBlockIds.Skip(1),
                    logList.Select(l => l.Items));
            }
            else
            {   //  Persist the CSV header only
                var header = TransactionItem.GetCsvHeader();
                var tx = new BookmarkTransaction(new[] { header }, null, null);
                var result = await bookmarkGateway.ApplyTransactionAsync(tx, ct);

                //  We do not need keep the CSV header block as we won't change it or delete it
                return new GlobalTableStatus(
                    bookmarkGateway,
                    new int[0],
                    new IEnumerable<TransactionItem>[0]);
            }
        }

        private GlobalTableStatus(
            BookmarkGateway bookmarkGateway,
            IEnumerable<int> blockIds,
            IEnumerable<IEnumerable<TransactionItem>> itemBlocks)
        {
            _bookmarkGateway = bookmarkGateway;

            //  Zip the ids with the data and index it
            var tableItemsList =
                blockIds.Zip(itemBlocks, (id, items) => KeyValuePair.Create(
                    new TableKey(items.First().KustoDatabaseName, items.First().KustoTableName),
                    new TableItemIndex(id, items)));

            //  Then index each of those per table / db
            _tableIndex = new ConcurrentDictionary<TableKey, TableItemIndex>(tableItemsList);
        }
        #endregion

        public TableStatus GetSingleTableStatus(string database, string table)
        {
            var tableKey = new TableKey(database, table);
            TableItemIndex? tableItemIndex;

            //  We know only one thread at the time would do this
            if (_tableIndex.TryGetValue(tableKey, out tableItemIndex))
            {
                return new TableStatus(this, database, table, tableItemIndex.ItemIndex.Values);
            }
            else
            {
                return new TableStatus(this, database, table, new TransactionItem[0]);
            }
        }

        internal async Task PersistNewItemsAsync(
            IEnumerable<TransactionItem> items,
            CancellationToken ct)
        {
            var itemsContent = TransactionItem.ToCsv(items);
            var tx = new BookmarkTransaction(new[] { itemsContent }, null, null);
            var result = await _bookmarkGateway.ApplyTransactionAsync(tx, ct);
            var newBlockId = result.AddedBlockIds.First();
            var tableKey = new TableKey(
                items.First().KustoDatabaseName,
                items.First().KustoTableName);
            TableItemIndex? tableItemIndex;

            //  We know only one thread at the time would do this
            if (!_tableIndex.TryGetValue(tableKey, out tableItemIndex))
            {
                _tableIndex[tableKey] = new TableItemIndex(newBlockId, items);
            }
            else
            {
                tableItemIndex.BlockIds.Add(newBlockId);
                foreach (var i in items)
                {   //  Keep latest added in memory
                    var transactionItemKey = new TransactionItemKey(
                        i.StartTxId,
                        i.EndTxId,
                        i.BlobPath);

                    tableItemIndex.ItemIndex[transactionItemKey] = i;
                }
            }
        }

        private static IEnumerable<TransactionItem> DeduplicateTransactionItems(
            IEnumerable<TransactionItem> items)
        {
            return items
                .GroupBy(i => new TransactionItemKey(i.StartTxId, i.EndTxId, i.BlobPath))
                .Select(g => g.First());
        }

        private ReadOnlyMemory<byte> ToBuffer(string headerText)
        {
            return new ReadOnlyMemory<byte>(ASCIIEncoding.UTF8.GetBytes(headerText));
        }
    }
}