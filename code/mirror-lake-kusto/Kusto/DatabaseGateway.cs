using Kusto.Data.Common;
using Kusto.Data.Ingestion;
using Kusto.Ingest;
using MirrorLakeKusto.Storage;
using System.Collections.Immutable;
using System.Data;

namespace MirrorLakeKusto.Kusto
{
    public class DatabaseGateway
    {
        private const string STATUS_VIEW_NAME = "MLK_DeltaStatus";
        
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

        public async Task<IImmutableList<T>> ExecuteQueryAsync<T>(
            string queryText,
            Func<IDataRecord, T> projection,
            ClientRequestProperties properties,
            CancellationToken ct)
        {
            return await _clusterGateway.ExecuteQueryAsync(
                DatabaseName,
                queryText,
                projection,
                properties,
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

        public async Task<IImmutableList<T>> ExecuteCommandAsync<T>(
            string commandText,
            Func<IDataRecord, T> projection,
            ClientRequestProperties properties,
            CancellationToken ct)
        {
            return await _clusterGateway.ExecuteCommandAsync(
                DatabaseName,
                commandText,
                projection,
                properties,
                ct);
        }

        public async Task CreateMergeDatabaseObjectsAsync(
            Uri checkpointBlobUrl,
            CancellationToken ct)
        {
            var columnListText = string.Join(
                ", ",
                TransactionItem.ExternalTableSchema
                .Split(',')
                .Select(c => c.Split(':').First()));
            var createStatusViewFunction = $@".create-or-alter function with
(docstring = 'Latest state view on checkpoint blob', folder='Kusto Mirror')
{STATUS_VIEW_NAME}{{
externaldata({TransactionItem.ExternalTableSchema})
[
   '{checkpointBlobUrl};impersonate'
]
with(format='csv', ignoreFirstRecord=true)
| summarize arg_max(MirrorTimestamp, *) by KustoTableName, StartTxId, Action, BlobPath
| order by KustoTableName asc, StartTxId asc, Action asc, BlobPath asc
| project {columnListText}
}}";
            var commandText = $@".execute database script with (ContinueOnErrors=false, ThrowOnErrors=true)<|
{createStatusViewFunction}";

            await ExecuteCommandAsync(
                commandText,
                r => 0,
                ct);
        }

        public KustoQueuedIngestionProperties GetIngestionProperties(string tableName)
        {
            return new KustoQueuedIngestionProperties(DatabaseName, tableName);
        }
    }
}