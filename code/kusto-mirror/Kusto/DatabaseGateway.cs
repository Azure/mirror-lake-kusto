using Kusto.Data.Common;
using Kusto.Data.Ingestion;
using Kusto.Ingest;
using Kusto.Mirror.ConsoleApp.Storage;
using System.Collections.Immutable;
using System.Data;

namespace Kusto.Mirror.ConsoleApp.Kusto
{
    public class DatabaseGateway
    {
        private readonly KustoClusterGateway _clusterGateway;

        public DatabaseGateway(KustoClusterGateway clusterGateway, string database)
        {
            _clusterGateway = clusterGateway;
            DatabaseName = database;
        }

        public string DatabaseName { get; }

        public async Task<IImmutableList<T>> ExecuteQueryAsync<T>(
            string queryText,
            Func<IDataRecord, T> projection,
            CancellationToken ct)
        {
            return await _clusterGateway.ExecuteQueryAsync(
                DatabaseName,
                queryText,
                projection,
                ct);
        }

        public async Task<IImmutableList<T>> ExecuteCommandAsync<T>(
            string commandText,
            Func<IDataRecord, T> projection,
            CancellationToken ct)
        {
            return await _clusterGateway.ExecuteCommandAsync(
                DatabaseName,
                commandText,
                projection,
                ct);
        }

        public async Task CreateMergeDatabaseObjectsAsync(
            Uri checkpointBlobUrl,
            CancellationToken ct)
        {
            var createStatusFunction = $@".create-or-alter function with
(docstring = 'View on checkpoint blob', folder='Kusto Mirror')
KM_DeltaStatus{{
externaldata({TransactionItem.ExternalTableSchema})
[
   '{checkpointBlobUrl};impersonate'
]
with(format='csv', ignoreFirstRecord=true)
}}";

            await ExecuteCommandAsync(
                createStatusFunction,
                r => 0,
                ct);
        }

        public async Task QueueIngestionAsync(
            Uri blobUrl,
            string tableName,
            DataSourceFormat format,
            IEnumerable<ColumnMapping> ingestionMappings,
            CancellationToken ct)
        {
            var properties = new KustoQueuedIngestionProperties(DatabaseName, tableName)
            {
                Format = format,
                IngestionMapping = new IngestionMapping()
                {
                    IngestionMappings = ingestionMappings,
                    IngestionMappingKind = TranslateMappingKind(format)
                }
            };

            await _clusterGateway.IngestFromStorageAsync(blobUrl, properties, ct);
        }

        private static IngestionMappingKind TranslateMappingKind(DataSourceFormat format)
        {
            switch (format)
            {
                case DataSourceFormat.csv:
                    return IngestionMappingKind.Csv;
                case DataSourceFormat.parquet:
                    return IngestionMappingKind.Parquet;
                case DataSourceFormat.json:
                    return IngestionMappingKind.Json;

                default:
                    throw new NotSupportedException($"Format '{format}' isn't supported");
            }
        }
    }
}