using Kusto.Ingest.Exceptions;
using System.Collections.Immutable;
using System.Text.Json;

namespace Kusto.Mirror.ConsoleApp.Storage
{
    internal class TransactionLog
    {
        public TransactionLog(
            TransactionItem? transactionMetadata,
            TransactionItem? transactionStagingTable,
            IEnumerable<TransactionItem> transactionAdds,
            IEnumerable<TransactionItem> transactionRemoves)
        {
            if (transactionMetadata == null
                && transactionStagingTable == null
                && !transactionAdds.Any()
                && !transactionRemoves.Any())
            {
                throw new ArgumentNullException(null, "There are no items");
            }
            Metadata = transactionMetadata;
            StagingTable = transactionStagingTable;
            Adds = transactionAdds.ToImmutableArray();
            Removes = transactionRemoves.ToImmutableArray();
        }

        public TransactionLog(IEnumerable<TransactionItem> items)
            : this(
                  items.Where(i => i.Action == TransactionItemAction.Schema).FirstOrDefault(),
                  items.Where(i => i.Action == TransactionItemAction.StagingTable).FirstOrDefault(),
                  items.Where(i => i.Action == TransactionItemAction.Add),
                  items.Where(i => i.Action == TransactionItemAction.Remove))
        {
        }

        public string KustoDatabaseName => AllItems.First().KustoDatabaseName;

        public string KustoTableName => AllItems.First().KustoTableName;

        public long StartTxId => AllItems.First().StartTxId;
        
        public long EndTxId => AllItems.First().EndTxId;

        public TransactionItem? Metadata { get; }

        public TransactionItem? StagingTable { get; }

        public IImmutableList<TransactionItem> Adds { get; }

        public IImmutableList<TransactionItem> Removes { get; }

        public IEnumerable<TransactionItem> AllItems
        {
            get
            {
                var all = Adds.Concat(Removes);

                return (Metadata != null)
                    ? all.Prepend(Metadata)
                    : all;
            }
        }

        public TransactionLog Coalesce(TransactionLog second)
        {
            if (second.Metadata != null)
            {
                throw new NotImplementedException();
            }
            if (StagingTable != null || second.StagingTable != null)
            {
                throw new NotImplementedException();
            }

            var addIndex = Adds
                .Select(r => r.BlobPath)
                .ToImmutableHashSet();
            var removeIndex = Removes
                .Select(r => r.BlobPath)
                .ToImmutableHashSet();
            var remainingAdds = Adds
                .Where(a => !removeIndex.Contains(a.BlobPath));
            var remainingRemoves = Removes
                .Where(a => !addIndex.Contains(a.BlobPath));

            return new TransactionLog(
                Metadata,
                null,
                remainingAdds,
                remainingRemoves);
        }

        public static TransactionLog Coalesce(IEnumerable<TransactionLog> txLogs)
        {
            var span = new Span<TransactionLog>(txLogs.ToArray());

            if (span.Length == 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(txLogs),
                    "Should contain at least of log");
            }
            else
            {
                return Coalesce(span[0], span.Slice(1));
            }
        }

        private static TransactionLog Coalesce(TransactionLog first, Span<TransactionLog> txLogs)
        {
            if (txLogs.Length == 0)
            {
                return first;
            }
            else
            {
                var second = txLogs[0];
                var merged = first.Coalesce(second);
                var remains = txLogs.Slice(1);

                return Coalesce(merged, remains);
            }
        }
    }
}