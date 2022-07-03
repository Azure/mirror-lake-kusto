using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kusto.Mirror.ConsoleApp.Storage.DeltaTable
{
    internal class DeltaTableGateway
    {
        private readonly Uri _deltaTableStorageUrl;
        private readonly BlobContainerClient _blobContainerClient;
        private readonly string _transactionFolderPrefix;

        public DeltaTableGateway(
            AuthenticationMode authenticationMode,
            Uri deltaTableStorageUrl)
        {
            var url = deltaTableStorageUrl;

            _deltaTableStorageUrl = url;

            if (url.Segments.Length < 2)
            {
                throw new ArgumentOutOfRangeException(
                    $"Url should contain the blob container:  {url}");
            }

            var credentials = CreateStorageCredentials(authenticationMode);
            var containerUrl =
                $"{url.Scheme}://{url.Host}{url.Segments[0]}{url.Segments[1]}";
            var tableFolder = string.Join(string.Empty, url.Segments.Skip(2));
            var transactionLogsFolder = $"{tableFolder}/_delta_log";

            _blobContainerClient = new BlobContainerClient(new Uri(containerUrl), credentials);
            _transactionFolderPrefix = transactionLogsFolder;
        }

        internal Task<object> GetLatestStateAsync(CancellationToken ct)
        {
            throw new NotImplementedException();
        }

        private static TokenCredential CreateStorageCredentials(
            AuthenticationMode authenticationMode)
        {
            switch (authenticationMode)
            {
                case AuthenticationMode.AppSecret:
                    throw new NotSupportedException();
                case AuthenticationMode.AzCli:
                    return new AzureCliCredential();
                case AuthenticationMode.Browser:
                    return new InteractiveBrowserCredential();

                default:
                    throw new NotSupportedException(
                        $"Unsupported authentication mode '{authenticationMode}'");
            }
        }

    }
}