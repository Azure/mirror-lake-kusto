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
        private const string STATUS_VIEW_NAME = "KM_DeltaStatus";
        private const string STATUS_LATEST_VIEW_NAME = "KM_DeltaStatusLatest";

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
            var createStatusViewFunction = $@".create-or-alter function with
(docstring = 'View on checkpoint blob', folder='Kusto Mirror')
{STATUS_VIEW_NAME}{{
externaldata({TransactionItem.ExternalTableSchema})
[
   '{checkpointBlobUrl};impersonate'
]
with(format='csv', ignoreFirstRecord=true)
| order by KustoDatabaseName asc, KustoTableName asc, StartTxId asc, State asc, Action asc, BlobPath asc, MirrorTimestamp asc
}}";
            var columnListText = string.Join(
                ", ",
                TransactionItem.ExternalTableSchema
                .Split(',')
                .Select(c => c.Split(':').First()));
            var createStatusLatestViewFunction = $@".create-or-alter function with
(docstring = 'Latest state view on checkpoint blob', folder='Kusto Mirror')
{STATUS_LATEST_VIEW_NAME}{{
{STATUS_VIEW_NAME}
| summarize arg_max(MirrorTimestamp, *) by KustoDatabaseName, KustoTableName, StartTxId, Action, State, BlobPath
| order by KustoDatabaseName asc, KustoTableName asc, StartTxId asc, Action asc, BlobPath asc
| project {columnListText}
}}";
            var commandText = $@".execute database script <|
{createStatusViewFunction}

{createStatusLatestViewFunction}";

            await ExecuteCommandAsync(
                commandText,
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