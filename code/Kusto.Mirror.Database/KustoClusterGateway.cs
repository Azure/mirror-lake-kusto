using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kusto.Mirror.Database
{
    public class KustoClusterGateway
    {
        private readonly ICslAdminProvider _commandProvider;
        private readonly ICslQueryProvider _queryProvider;
        private readonly Uri _clusterUri;
        private readonly string _application;
        private readonly IImmutableList<KeyValuePair<string, object>>? _requestOptions;

        public KustoClusterGateway(
            Uri clusterUri,
            string version,
            string? requestDescription = null)
        {
            var builder = new KustoConnectionStringBuilder(clusterUri.ToString());
            var kustoConnectionStringBuilder = builder.WithAadAzCliAuthentication(false);

            _commandProvider =
                KustoClientFactory.CreateCslCmAdminProvider(kustoConnectionStringBuilder);
            _queryProvider =
                KustoClientFactory.CreateCslQueryProvider(kustoConnectionStringBuilder);
            _clusterUri = clusterUri;
            _application = $"Kusto-Mirror;{version}";
            if (requestDescription != null)
            {
                _requestOptions = ImmutableArray<KeyValuePair<string, object>>
                    .Empty
                    .Add(KeyValuePair.Create(
                        ClientRequestProperties.OptionRequestDescription,
                        (object)requestDescription));
            }
        }
        public async Task<IDataReader> ExecuteCommandAsync(
            string database,
            string commandText,
            CancellationToken ct)
        {
            var reader = await _commandProvider.ExecuteControlCommandAsync(
                database,
                commandText,
                new ClientRequestProperties(_requestOptions, null)
                {
                    Application = _application
                });

            return reader;
        }
    }
}