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

                if (Metadata != null)
                {
                    all = all.Append(Metadata);
                }
                if (StagingTable != null)
                {
                    all = all.Append(StagingTable);
                }

                return all;
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
            Action<TransactionItem> action = clone =>
            {
                clone.StartTxId = Math.Min(StartTxId, second.StartTxId);
                clone.EndTxId = Math.Max(EndTxId, second.EndTxId);
            };
            var remainingAdds = Adds
                .Where(a => !removeIndex.Contains(a.BlobPath))
                .Select(a => a.Clone(c => action(c)));
            var remainingRemoves = Removes
                .Where(a => !addIndex.Contains(a.BlobPath))
                .Select(a => a.Clone(c => action(c)));

            return new TransactionLog(
                Metadata != null ? Metadata.Clone(action) : null,
                null,
                remainingAdds,
                remainingRemoves);
        }

        public TransactionLog Delta(TransactionLog currentLog)
        {
            throw new NotImplementedException();
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