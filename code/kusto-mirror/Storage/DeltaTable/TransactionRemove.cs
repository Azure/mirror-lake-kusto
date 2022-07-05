using System.Collections.Immutable;

namespace Kusto.Mirror.ConsoleApp.Storage.DeltaTable
{
    internal class TransactionRemove
    {
        public TransactionRemove(TransactionLogEntry.RemoveData removeEntry)
        {
            if (string.IsNullOrWhiteSpace(removeEntry.Path))
            {
                throw new ArgumentNullException(nameof(removeEntry.Path));
            }
            if (removeEntry.PartitionValues == null)
            {
                throw new ArgumentNullException(nameof(removeEntry.PartitionValues));
            }
            Path = removeEntry.Path;
            DeletionTimestamp = DateTimeOffset
                .FromUnixTimeMilliseconds(removeEntry.DeletionTimestamp)
                .UtcDateTime;
            DataChange = removeEntry.DataChange;
            PartitionValues = removeEntry.PartitionValues.ToImmutableDictionary(
                p => p.Key,
                p => p.Value);
            Size = removeEntry.Size;
        }

        public string Path { get; }

        public DateTime DeletionTimestamp { get; }

        public bool DataChange { get; }

        public IImmutableDictionary<string, string> PartitionValues { get; }

        public long Size { get; }
    }
}