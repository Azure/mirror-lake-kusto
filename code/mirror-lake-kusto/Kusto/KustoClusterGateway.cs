using CommandLine;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Ingest;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MirrorLakeKusto.Kusto
{
    public class KustoClusterGateway
    {
        private readonly static ClientRequestProperties EMPTY_REQUEST_PROPERTIES =
            new ClientRequestProperties();

        private readonly ICslQueryProvider _queryProvider;
        private readonly ICslAdminProvider _commandProvider;
        private readonly IKustoQueuedIngestClient _ingestionProvider;
        private readonly string _application;
        private readonly IImmutableList<KeyValuePair<string, object>>? _requestOptions;

        #region Constructors
        public static async Task<KustoClusterGateway> CreateAsync(
            string clusterIngestionConnectionString,
            string version,
            string? requestDescription = null)
        {
            var ingestionStringBuilder =
                new KustoConnectionStringBuilder(clusterIngestionConnectionString);
            var queryStringBuilder =
                new KustoConnectionStringBuilder(clusterIngestionConnectionString);

            if (Uri.TryCreate(clusterIngestionConnectionString, UriKind.Absolute, out _))
            {   //  Enforce Az CLI authentication if user simply provides cluster ingestion URI
                ingestionStringBuilder = ingestionStringBuilder.WithAadUserPromptAuthentication();
                queryStringBuilder = queryStringBuilder.WithAadUserPromptAuthentication();
            }

            var clusterQueryUri = await GetQueryUriAsync(ingestionStringBuilder);

            queryStringBuilder.DataSource = clusterQueryUri.ToString();

            return new KustoClusterGateway(
                ingestionStringBuilder,
                queryStringBuilder,
                version,
                requestDescription);
        }

        private KustoClusterGateway(
            KustoConnectionStringBuilder ingestionStringBuilder,
            KustoConnectionStringBuilder queryStringBuilder,
            string version,
            string? requestDescription = null)
        {
            _queryProvider = KustoClientFactory.CreateCslQueryProvider(queryStringBuilder);
            _commandProvider = KustoClientFactory.CreateCslCmAdminProvider(queryStringBuilder);
            _ingestionProvider = KustoIngestFactory.CreateQueuedIngestClient(
                ingestionStringBuilder);
            if (requestDescription != null)
            {
                _application = $"Kusto-Mirror;{version}";
                _requestOptions = ImmutableArray<KeyValuePair<string, object>>
                    .Empty
                    .Add(KeyValuePair.Create(
                        ClientRequestProperties.OptionRequestDescription,
                        (object)requestDescription));
            }
            else
            {
                _application = string.Empty;
            }
        }
        #endregion

        public async Task<bool> IsFreeClusterAsync(CancellationToken ct)
        {
            var results = await ExecuteCommandAsync(
                string.Empty,
                ".show version | project IsFree = toint(ServiceOffering has 'Personal')",
                r => (int)r[0] != 0,
                ct);

            return results.First();
        }

        internal async Task IngestFromStorageAsync(
            Uri blobUrl,
            KustoQueuedIngestionProperties properties,
            CancellationToken ct)
        {
            var ingestionBlobUrl = $"{blobUrl};managed_identity=system";
            var result = await _ingestionProvider.IngestFromStorageAsync(
                ingestionBlobUrl,
                properties,
                //  Recommended so the DM sizes the file properly
                new StorageSourceOptions { Size = 0 });
            var failureStatus = result.GetIngestionStatusCollection().First();

            if (failureStatus.Status != Status.Queued)
            {
                throw new MirrorException(
                    $"Blob '{blobUrl}' hasn't queued ; state '{failureStatus.Status}', "
                    + $"failure status '{failureStatus.FailureStatus}' & "
                    + $"error code '{failureStatus.ErrorCode}'");
            }
        }

        public async Task<IImmutableList<T>> ExecuteQueryAsync<T>(
            string database,
            string queryText,
            Func<IDataRecord, T> projection,
            CancellationToken ct)
        {
            return await ExecuteQueryAsync<T>(
                database,
                queryText,
                projection,
                EMPTY_REQUEST_PROPERTIES,
                ct);
        }
        
        public async Task<IImmutableList<T>> ExecuteQueryAsync<T>(
            string database,
            string queryText,
            Func<IDataRecord, T> projection,
            ClientRequestProperties properties,
            CancellationToken ct)
        {
            try
            {
                using (var reader = await _queryProvider.ExecuteQueryAsync(
                    database,
                    queryText,
                    _requestOptions != null
                    ? new ClientRequestProperties(
                        _requestOptions.Concat(properties.Options),
                        properties.Parameters)
                    {
                        Application = _application
                    }
                    : properties))
                {
                    var output = Project(reader, projection)
                        .ToImmutableArray();

                    return output;
                }
            }
            catch (Exception ex)
            {
                throw new MirrorException(
                    $"Issue running the query '{queryText}' on database '{database}'",
                    ex);
            }
        }

        public async Task<IImmutableList<T>> ExecuteCommandAsync<T>(
            string database,
            string commandText,
            Func<IDataRecord, T> projection,
            CancellationToken ct)
        {
            return await ExecuteCommandAsync<T>(
                database,
                commandText,
                projection,
                EMPTY_REQUEST_PROPERTIES,
                ct);
        }
        
        public async Task<IImmutableList<T>> ExecuteCommandAsync<T>(
            string database,
            string commandText,
            Func<IDataRecord, T> projection,
            ClientRequestProperties properties,
            CancellationToken ct)
        {
            try
            {
                using (var reader = await _commandProvider.ExecuteControlCommandAsync(
                    database,
                    commandText,
                    _requestOptions != null
                    ? new ClientRequestProperties(
                        _requestOptions.Concat(properties.Options),
                        properties.Parameters)
                    {
                        Application = _application
                    }
                    : properties))
                {
                    var output = Project(reader, projection)
                        .ToImmutableArray();

                    return output;
                }
            }
            catch (Exception ex)
            {
                throw new MirrorException(
                    $"Issue running the command '{commandText}' on database '{database}'",
                    ex);
            }
        }

        private static async Task<Uri> GetQueryUriAsync(
            KustoConnectionStringBuilder ingestionStringBuilder)
        {
            try
            {
                var dmCommandProvider =
                    KustoClientFactory.CreateCslCmAdminProvider(ingestionStringBuilder);
                var dataReader = await dmCommandProvider.ExecuteControlCommandAsync(
                    "",
                    ".show query service uri");
                var clusterQueryUrl = Project(dataReader, r => r.GetString(0)).First();
                var clusterQueryUri = new Uri(clusterQueryUrl);

                return clusterQueryUri;
            }
            catch (Exception ex)
            {
                throw new MirrorException("Error retrieving the cluster query uri", ex);
            }
        }

        private static IEnumerable<T> Project<T>(
            IDataReader reader,
            Func<IDataRecord, T> projection)
        {
            while (reader.Read())
            {
                yield return projection(reader);
            }
        }
    }
}