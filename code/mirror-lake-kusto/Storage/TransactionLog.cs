using Kusto.Cloud.Platform.Utils;
using Kusto.Ingest.Exceptions;
using System;
using System.Collections.Immutable;
using System.Text.Json;

namespace MirrorLakeKusto.Storage
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

            var allAdds = Adds.Concat(second.Adds);
            var allRemoves = Removes.Concat(second.Removes);
            var allStagingTables = new[] { StagingTable, second.StagingTable };
            var remainingStagingTables = allStagingTables
                .Where(s => s != null && s.State != TransactionItemState.Done);
            var addIndex = allAdds
                .Select(r => r.BlobPath)
                .ToImmutableHashSet();
            var removeIndex = allRemoves
                .Select(r => r.BlobPath)
                .ToImmutableHashSet();
            Action<TransactionItem> action = clone =>
            {
                clone.StartTxId = Math.Min(StartTxId, second.StartTxId);
                clone.EndTxId = Math.Max(EndTxId, second.EndTxId);
            };
            var remainingAdds = allAdds
                .Where(a => !removeIndex.Contains(a.BlobPath));
            var remainingRemoves = allRemoves
                .Where(a => !addIndex.Contains(a.BlobPath));
            var newAdds = remainingAdds
                .Select(a => a.Clone(c => action(c)));
            var newRemoves = Removes
                .Select(a => a.Clone(c => action(c)));

            if (remainingStagingTables.Count() > 1)
            {
                throw new NotSupportedException();
            }

            return new TransactionLog(
                Metadata != null ? Metadata.Clone(action) : null,
                remainingStagingTables.FirstOrDefault(),
                newAdds,
                newRemoves);
        }

        public TransactionLog Delta(TransactionLog previousLog)
        {
            var currentAdds = Adds.ToImmutableDictionary(a => a.BlobPath!, a => a);
            var previousAdds = previousLog.Adds.ToImmutableDictionary(a => a.BlobPath!, a => a);
            var currentRemoves = Removes.ToImmutableDictionary(r => r.BlobPath!, r => r);
            var previousRemoves = previousLog.Removes
                .ToImmutableDictionary(r => r.BlobPath!, r => r);
            var newAdds = currentAdds
                .Where(p => !previousAdds.ContainsKey(p.Key))
                .Select(p => p.Value)
                .ToImmutableArray();
            var newRemoveBlobPaths = previousAdds
                .Where(p => !currentAdds.ContainsKey(p.Key))
                .Where(p => !currentRemoves.ContainsKey(p.Key))
                .Select(p => p.Key)
                .Concat(currentRemoves.Keys)
                .ToImmutableHashSet();
            var newRemoves = newRemoveBlobPaths
                .Select(p => previousAdds[p])
                .ToImmutableArray();
            var brokenRemoveBlobPaths = previousRemoves
                .Where(p => !newRemoveBlobPaths.Contains(p.Key))
                .ToImmutableHashSet();
            var allStagingTables = new[] { StagingTable, previousLog.StagingTable };
            var remainingStagingTables = allStagingTables
                .Where(s => s != null && s.State != TransactionItemState.Done);
            var assignCurrentTx = (TransactionItem i) =>
            {
                i.StartTxId = StartTxId;
                i.EndTxId = EndTxId;
            };

            if (brokenRemoveBlobPaths.Any())
            {
                throw new MirrorException("Log-delta missing past removes:  "
                    + string.Join(", ", brokenRemoveBlobPaths));
            }
            if (!previousLog.Metadata!.PartitionColumns!.SequenceEqual(Metadata!.PartitionColumns!)
                || !previousLog.Metadata!.Schema!.SequenceEqual(Metadata!.Schema!))
            {
                throw new NotSupportedException("Schema changed unsupported");
            }

            return new TransactionLog(
                Metadata != null ? Metadata.Clone(assignCurrentTx) : null,
                remainingStagingTables.Select(i => i!.Clone(assignCurrentTx)).FirstOrDefault(),
                newAdds.Select(i => i.Clone(assignCurrentTx)),
                newRemoves.Select(i => i.Clone(assignCurrentTx)));
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