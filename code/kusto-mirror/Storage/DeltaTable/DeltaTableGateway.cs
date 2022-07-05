using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
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
            _deltaTableStorageUrl = deltaTableStorageUrl;

            if (_deltaTableStorageUrl.Segments.Length < 2)
            {
                throw new ArgumentOutOfRangeException(
                    $"Url should contain the blob container:  {_deltaTableStorageUrl}");
            }

            var credentials = CreateStorageCredentials(authenticationMode);
            var transactionLogsFolder = $"{_deltaTableStorageUrl}/_delta_log";
            var builder = new BlobUriBuilder(new Uri(transactionLogsFolder));

            //  Enforce blob storage API
            builder.Host =
                builder.Host.Replace(".dfs.core.windows.net", ".blob.core.windows.net");
            //  No SAS support
            builder.Sas = null;
            //  Capture blob name
            _transactionFolderPrefix = builder.BlobName;
            //  Remove blob name to capture the container URI only
            builder.BlobName = string.Empty;

            _blobContainerClient = new BlobContainerClient(builder.ToUri(), credentials);
        }

        internal async Task<IImmutableList<TransactionLog>> GetTransactionLogsAsync(
            int? fromTxId,
            CancellationToken ct)
        {
            var lastCheckpointName = $"{_transactionFolderPrefix}/_last_checkpoint";
            var lastCheckpointBlob = _blobContainerClient.GetBlobClient(lastCheckpointName);
            var lastCheckpointExists = await lastCheckpointBlob.ExistsAsync();

            //if (lastCheckpointExists)
            //{
            //    throw new NotImplementedException();
            //}
            //else
            {
                var blobPageable = _blobContainerClient.GetBlobsAsync(
                    BlobTraits.None,
                    BlobStates.None,
                    _transactionFolderPrefix,
                    ct);
                var blobItems = await blobPageable.ToListAsync();
                var txLogTasks = blobItems
                    .Select(b => new
                    {
                        b.Name,
                        TxId = ExtractTransactionId(b.Name)
                    })
                    .Where(c => c.TxId.HasValue)
                    .OrderBy(c => c.TxId!.Value)
                    .Select(c => LoadTransactionBlobAsync(c.TxId!.Value, c.Name))
                    .ToImmutableArray();

                await Task.WhenAll(txLogTasks);

                var txLogs = txLogTasks
                    .Select(t => t.Result)
                    .ToImmutableArray();

                return txLogs;
            }
        }

        private async Task<TransactionLog> LoadTransactionBlobAsync(int txId, string name)
        {
            try
            {
                var blobClient = _blobContainerClient.GetBlockBlobClient(name);
                var downloadResult = await blobClient.DownloadContentAsync();
                var blobText = downloadResult.Value.Content.ToString();

                return TransactionLog.LoadLog(txId, blobText);
            }
            catch (Exception ex)
            {
                throw new MirrorException($"Error loading transaction log '{name}'", ex);
            }
        }

        private int? ExtractTransactionId(string blobName)
        {
            if (blobName.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
            {
                var lastSlash = blobName.LastIndexOf('/');
                var lastDot = blobName.LastIndexOf('.');
                var txIdText = blobName.Substring(lastSlash + 1, lastDot - lastSlash - 1);
                int txId;

                if (int.TryParse(txIdText, out txId))
                {
                    return txId;
                }
            }

            return null;
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