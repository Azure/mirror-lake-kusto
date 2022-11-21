using Azure.Core;
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
        private readonly string _application;
        private readonly IImmutableList<KeyValuePair<string, object>>? _requestOptions;

        #region Constructors
        public static KustoClusterGateway Create(
            string clusterQueryConnectionString,
            TokenCredential credentials,
            string version,
            string? requestDescription = null)
        {
            var queryStringBuilder =
                new KustoConnectionStringBuilder(clusterQueryConnectionString);

            queryStringBuilder =
                queryStringBuilder.WithAadAzureTokenCredentialsAuthentication(credentials);

            return new KustoClusterGateway(
                queryStringBuilder,
                version,
                requestDescription);
        }

        private KustoClusterGateway(
            KustoConnectionStringBuilder queryStringBuilder,
            string version,
            string? requestDescription = null)
        {
            _queryProvider = KustoClientFactory.CreateCslQueryProvider(queryStringBuilder);
            _commandProvider = KustoClientFactory.CreateCslAdminProvider(queryStringBuilder);
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