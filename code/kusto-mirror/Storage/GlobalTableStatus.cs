using Azure.Core;
using Kusto.Mirror.ConsoleApp.Storage.Bookmark;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kusto.Mirror.ConsoleApp.Storage
{
    internal class GlobalTableStatus
    {
        private readonly BookmarkGateway _bookmarkGateway;

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
            {
                var content = await bookmarkGateway.ReadAllContentAsync(ct);
                var items = TransactionItem.FromCsv(content, true);

                throw new NotImplementedException();
            }

            return new GlobalTableStatus(bookmarkGateway);
        }

        private GlobalTableStatus(BookmarkGateway bookmarkGateway)
        {
            _bookmarkGateway = bookmarkGateway;
        }
        #endregion

        public TableStatus GetSingleTableStatus(string database, string table)
        {
            return new TableStatus(this, database, table, new TransactionItem[0]);
        }

        public async Task PersistNewBatchAsync(TransactionLog log, CancellationToken ct)
        {
            var header = TransactionItem.GetCsvHeader();
            var allItems = TransactionItem.ToCsv(log.AllItems);
            var tx = new BookmarkTransaction(new[] { header, allItems }, null, null);

            await _bookmarkGateway.ApplyTransactionAsync(tx, ct);
        }

        private ReadOnlyMemory<byte> ToBuffer(string headerText)
        {
            return new ReadOnlyMemory<byte>(ASCIIEncoding.UTF8.GetBytes(headerText));
        }
    }
}