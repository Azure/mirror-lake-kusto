using System.Collections.Immutable;
using System.Text.Json;

namespace Kusto.Mirror.ConsoleApp.Storage.DeltaTable
{
    internal class TransactionAdd
    {
        public TransactionAdd(TransactionLogEntry.AddData addEntry)
        {
            if (string.IsNullOrWhiteSpace(addEntry.Path))
            {
                throw new ArgumentNullException(nameof(addEntry.Path));
            }
            if (addEntry.PartitionValues == null)
            {
                throw new ArgumentNullException(nameof(addEntry.PartitionValues));
            }
            Path = addEntry.Path;
            PartitionValues = addEntry.PartitionValues.ToImmutableDictionary(
                p => p.Key,
                p => p.Value);
            Size = addEntry.Size;
            ModificationTime = DateTimeOffset
                .FromUnixTimeMilliseconds(addEntry.ModificationTime)
                .UtcDateTime;
            DataChange = addEntry.DataChange;
            RecordCount = ExtractRecordCount(addEntry.Stats);
        }

        public string Path { get; }

        public IImmutableDictionary<string, string> PartitionValues { get; }

        public long Size { get; }

        public DateTime ModificationTime { get; }

        public bool DataChange { get; }

        public long RecordCount { get; }

        private static long ExtractRecordCount(string stats)
        {
            var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
            var statsData =
                JsonSerializer.Deserialize<TransactionLogEntry.AddData.StatsData>(
                    stats,
                    options);

            if (statsData == null)
            {
                throw new ArgumentException(
                    $"Incorrect format:  '{stats}'",
                    nameof(stats));
            }

            return statsData.NumRecords;
        }
    }
}