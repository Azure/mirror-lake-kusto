using MirrorLakeKusto.Kusto;
using MirrorLakeKusto.Storage;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Reflection.Metadata;
using YamlDotNet.Core.Tokens;

namespace MirrorLakeKusto.Orchestrations
{
    internal class BlobAnalysisOrchestration
    {
        #region Inner types
        private class PartitionValuesComparer
            : IEqualityComparer<IImmutableDictionary<string, string>>
        {
            bool IEqualityComparer<IImmutableDictionary<string, string>>.Equals(
                IImmutableDictionary<string, string>? x,
                IImmutableDictionary<string, string>? y)
            {
                if ((x == null && y != null) || (x != null && y == null))
                {
                    return false;
                }
                else if (x == null || y == null)
                {
                    return true;
                }
                else if (x.Count != y.Count)
                {
                    return false;
                }
                else
                {
                    foreach (var pairX in x)
                    {
                        if (!y.TryGetValue(pairX.Key, out var valueY)
                            || pairX.Value != valueY)
                        {
                            return false;
                        }
                    }

                    return true;
                }
            }

            int IEqualityComparer<IImmutableDictionary<string, string>>.GetHashCode(
                IImmutableDictionary<string, string> obj)
            {
                if (obj.Any())
                {
                    //  XOR all hash codes
                    var hashcode = obj
                        .Select(p => p.Key.GetHashCode() ^ p.Value.GetHashCode())
                        .Aggregate((h1, h2) => h1 ^ h2);

                    return hashcode;
                }
                else
                {
                    return 0;
                }
            }
        }
        #endregion

        private static readonly TimeSpan DELAY_BETWEEN_PERSIST = TimeSpan.FromSeconds(5);

        private readonly DatabaseGateway _databaseGateway;
        private readonly TableDefinition _stagingTable;
        private readonly TableStatus _tableStatus;
        private readonly IImmutableList<TransactionItem> _itemsToIngest;

        #region Constructors
        public static async Task EnsureAllAnalyzedAsync(
            DatabaseGateway databaseGateway,
            TableDefinition stagingTable,
            TableStatus tableStatus,
            long startTxId,
            CancellationToken ct)
        {
            var orchestration = new BlobAnalysisOrchestration(
                databaseGateway,
                stagingTable,
                tableStatus,
                startTxId);

            await orchestration.RunAsync(ct);
        }

        private BlobAnalysisOrchestration(
            DatabaseGateway databaseGateway,
            TableDefinition stagingTable,
            TableStatus tableStatus,
            long startTxId)
        {
            var logs = tableStatus.GetBatch(startTxId);
            var itemsToIngest = logs.Adds
                .Where(i => i.State != TransactionItemState.Analyzed)
                .ToImmutableArray();

            _databaseGateway = databaseGateway;
            _stagingTable = stagingTable;
            _tableStatus = tableStatus;
            _itemsToIngest = itemsToIngest;
        }
        #endregion

        #region Orchestration
        private async Task RunAsync(CancellationToken ct)
        {
            Trace.WriteLine($"Analyzing {_itemsToIngest.Count()} blobs");

            await Task.CompletedTask;
        }
        #endregion
    }
}