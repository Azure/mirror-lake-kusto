using System.Collections.Immutable;
using System.Text.Json;

namespace Kusto.Mirror.ConsoleApp.Storage.DeltaTable
{
    internal class TransactionLog
    {
        public static TransactionLog LoadLog(int txId, string blobText)
        {
            var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
            var lines = blobText.Split('\n');
            var entries = lines
                .Where(l => !string.IsNullOrWhiteSpace(l))
                .Select(l => JsonSerializer.Deserialize<TransactionLogEntry>(l, options))
                .ToImmutableArray();

            throw new NotImplementedException();
        }

        public TransactionLog()
        {
        }

        public int StartTxId { get; }

        public int EndTxId { get; }
    }
}