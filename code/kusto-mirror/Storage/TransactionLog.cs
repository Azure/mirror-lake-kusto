using System.Collections.Immutable;
using System.Text.Json;

namespace Kusto.Mirror.ConsoleApp.Storage
{
    internal class TransactionLog
    {
        public TransactionLog(
            TransactionItem? transactionMetadata,
            IEnumerable<TransactionItem> transactionAdds,
            IEnumerable<TransactionItem> transactionRemoves)
        {
            if(transactionMetadata == null
                && !transactionAdds.Any()
                && !transactionRemoves.Any())
            {
                throw new ArgumentNullException(null, "There are no items");
            }
            Metadata = transactionMetadata;
            Adds = transactionAdds.ToImmutableArray();
            Removes = transactionRemoves.ToImmutableArray();
        }

        public string KustoDatabaseName => AllItems.First().KustoDatabaseName;

        public string KustoTableName => AllItems.First().KustoTableName;

        public TransactionItem? Metadata { get; }

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