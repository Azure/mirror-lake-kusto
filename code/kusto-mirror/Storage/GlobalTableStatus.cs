using Azure.Core;
using Kusto.Mirror.ConsoleApp.Storage.Bookmark;
using System;
using System.Collections.Generic;
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
    }
}