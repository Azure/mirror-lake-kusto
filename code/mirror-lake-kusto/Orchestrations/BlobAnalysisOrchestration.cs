using Kusto.Data.Common;
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
            : IEqualityComparer<IImmutableList<string>>
        {
            bool IEqualityComparer<IImmutableList<string>>.Equals(
                IImmutableList<string>? x,
                IImmutableList<string>? y)
            {
                if ((x == null && y != null) || (x != null && y == null))
                {
                    return false;
                }
                else if (x == null || y == null)
                {
                    return true;
                }
                else
                {
                    return Enumerable.SequenceEqual(x, y);
                }
            }

            int IEqualityComparer<IImmutableList<string>>.GetHashCode(
                IImmutableList<string> obj)
            {
                if (obj.Any())
                {
                    //  XOR all hash codes
                    var hashcode = obj
                        .Select(p => p.GetHashCode() ^ p.GetHashCode())
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
        private readonly string? _creationTimeExpression;
        private readonly IImmutableList<TransactionItem> _itemsToAnalyze;

        #region Constructors
        public static async Task EnsureAllAnalyzedAsync(
            DatabaseGateway databaseGateway,
            TableDefinition stagingTable,
            TableStatus tableStatus,
            long startTxId,
            string? creationTimeExpression,
            CancellationToken ct)
        {
            var orchestration = new BlobAnalysisOrchestration(
                databaseGateway,
                stagingTable,
                tableStatus,
                startTxId,
                creationTimeExpression);

            await orchestration.RunAsync(ct);
        }

        private BlobAnalysisOrchestration(
            DatabaseGateway databaseGateway,
            TableDefinition stagingTable,
            TableStatus tableStatus,
            long startTxId,
            string? creationTimeExpression)
        {
            var logs = tableStatus.GetBatch(startTxId);
            var itemsToAnalyze = logs.Adds
                .Where(i => i.State != TransactionItemState.Analyzed)
                .ToImmutableArray();

            _databaseGateway = databaseGateway;
            _stagingTable = stagingTable;
            _tableStatus = tableStatus;
            _creationTimeExpression = creationTimeExpression;
            _itemsToAnalyze = itemsToAnalyze;
        }
        #endregion

        #region Orchestration
        private async Task RunAsync(CancellationToken ct)
        {
            var itemsToAnalyze = _itemsToAnalyze;

            if (itemsToAnalyze.Any())
            {
                Trace.WriteLine($"Analyzing {_itemsToAnalyze.Count()} blobs");

                if (_creationTimeExpression != null
                    && _stagingTable.PartitionColumns != null
                    && _stagingTable.PartitionColumns.Count() > 0)
                {
                    itemsToAnalyze = await AnalyzeCreationTimesAsync(itemsToAnalyze, ct);
                }

                var newItems = itemsToAnalyze
                    .Select(i => i.UpdateState(TransactionItemState.Analyzed))
                    .ToImmutableArray();

                await _tableStatus.PersistNewItemsAsync(newItems, ct);
            }
        }
        #endregion

        private async Task<IImmutableList<TransactionItem>> AnalyzeCreationTimesAsync(
            IImmutableList<TransactionItem> itemsToAnalyze,
            CancellationToken ct)
        {
            var items = itemsToAnalyze
                .Select(i => new
                {
                    Item = i,
                    PartitionArray = GetPartitionArray(i.PartitionValues)
                })
                .ToImmutableArray();
            var partitionValues = items
                .Where(i => i.PartitionArray != null)
                .Select(i => i.PartitionArray!)
                .Distinct(new PartitionValuesComparer())
                .ToImmutableArray();
            var creationTimes = await ComputeCreationTimesAsync(partitionValues, ct);
            var creationTimeMap = partitionValues
                .Zip(creationTimes, (p, t) => new { Values = p, Time = t })
                .ToImmutableDictionary(z => z.Values, z => z.Time, new PartitionValuesComparer());
            var newItems = items
                .Select(i => creationTimeMap.TryGetValue(i.PartitionArray!, out var time)
                ? i.Item.Clone(j => j.InternalState!.AddInternalState!.CreationTime = time)
                : i.Item)
                .ToImmutableList();

            return newItems;
        }

        private async Task<IImmutableList<DateTime>> ComputeCreationTimesAsync(
            IImmutableList<IImmutableList<string>> partitionValues,
            CancellationToken ct)
        {
            if (partitionValues.Any())
            {
                var partitionCount = partitionValues.First().Count();
                var properties = new ClientRequestProperties();
                var parameterDeclarations = Enumerable.Range(0, partitionValues.Count())
                    .Select(i => Enumerable.Range(0, partitionCount).Select(j => new { i, j }))
                    .SelectMany(p => p)
                    .Select(p => $"part_{p.i}_{p.j}:string");
                var parameterDeclarationText = string.Join(", ", parameterDeclarations);
                var rowConstruct = Enumerable.Range(0, partitionValues.Count())
                    .Select(i => Enumerable.Range(0, partitionCount).Select(j => new { i, j }))
                    .Select(a => a.Select(p => $"p{p.j}=part_{p.i}_{p.j}"))
                    .Select(a => $"(print {string.Join(", ", a)})");

                for (int i = 0; i != partitionValues.Count(); ++i)
                {
                    for (int j = 0; j < partitionValues[i].Count(); ++j)
                    {
                        properties.SetParameter($"part_{i}_{j}", (partitionValues[i])[j]);
                    }
                }
                var queryText = @$"declare query_parameters({parameterDeclarationText});
{string.Join("| union ", rowConstruct)}
| project Result={_creationTimeExpression}";

                try
                {
                    var results = await _databaseGateway.ExecuteQueryAsync(
                        queryText,
                        r => (DateTime)r["Result"],
                        properties,
                        ct);

                    return results;
                }
                catch (Exception ex)
                {
                    throw new MirrorException("Issue computing creation time in Kusto", ex);
                }
            }
            else
            {
                return ImmutableArray<DateTime>.Empty;
            }
        }

        private IImmutableList<string>? GetPartitionArray(
            IImmutableDictionary<string, string>? partitionValues)
        {
            if (partitionValues != null
                && partitionValues.Count() == _stagingTable.PartitionColumns.Count())
            {
                var absentColumns = _stagingTable.PartitionColumns
                    .Where(c => !partitionValues.ContainsKey(c));

                if (!absentColumns.Any())
                {
                    var array = _stagingTable.PartitionColumns
                        .Select(c => partitionValues[c])
                        .ToImmutableArray();

                    return array;
                }
            }

            return null;
        }
    }
}