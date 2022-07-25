using Azure.Core;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Kusto.Mirror.ConsoleApp.Storage.DeltaLake
{
    internal class DeltaTableGateway
    {
        #region Inner Types
        private class Checkpoint
        {
            public long Version { get; set; }

            public long Size { get; set; }
        }
        #endregion

        private readonly BlobContainerClient _blobContainerClient;
        private readonly string _transactionFolderPrefix;

        public DeltaTableGateway(
            TokenCredential storageCredentials,
            Uri deltaTableStorageUrl)
        {
            DeltaTableStorageUrl = deltaTableStorageUrl;

            if (DeltaTableStorageUrl.Segments.Length < 2)
            {
                throw new ArgumentOutOfRangeException(
                    $"Url should contain the blob container:  {DeltaTableStorageUrl}");
            }

            var transactionLogsFolder = $"{DeltaTableStorageUrl}/_delta_log";
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

            _blobContainerClient = new BlobContainerClient(builder.ToUri(), storageCredentials);
        }

        public Uri DeltaTableStorageUrl { get; }

        internal async Task<IImmutableList<TransactionLog>> GetTransactionLogsAsync(
            int? fromTxId,
            string kustoDatabaseName,
            string kustoTableName,
            CancellationToken ct)
        {
            var lastCheckpointName = $"{_transactionFolderPrefix}/_last_checkpoint";
            var lastCheckpointBlob = _blobContainerClient.GetBlobClient(lastCheckpointName);
            var lastCheckpointExists = await lastCheckpointBlob.ExistsAsync();

            if (lastCheckpointExists.Value)
            {
                var cummulativeTxLog = await ExtractCummulativeTxLogAsync(
                    lastCheckpointBlob,
                    kustoDatabaseName,
                    kustoTableName);

                throw new NotImplementedException();
            }
            else
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
                    .Where(c => fromTxId == null || c.TxId >= fromTxId)
                    .OrderBy(c => c.TxId!.Value)
                    .Select(c => LoadTransactionBlobAsync(
                        c.TxId!.Value,
                        c.Name,
                        kustoDatabaseName,
                        kustoTableName))
                    .ToImmutableArray();

                await Task.WhenAll(txLogTasks);

                var txLogs = txLogTasks
                    .Select(t => t.Result)
                    .ToImmutableArray();

                return txLogs;
            }
        }

        private async Task<TransactionLog> ExtractCummulativeTxLogAsync(
            BlobClient lastCheckpointBlob,
            string kustoDatabaseName,
            string kustoTableName)
        {
            var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
            var checkpointResult = await lastCheckpointBlob.DownloadContentAsync();
            var checkpointText = checkpointResult.Value.Content.ToString();
            var checkpoint = JsonSerializer.Deserialize<Checkpoint>(checkpointText, options);
            var paddedVersion = checkpoint!.Version.ToString("D20");
            var parquetName = $"{_transactionFolderPrefix}/{paddedVersion}.checkpoint.parquet";
            var parquetBlob = _blobContainerClient.GetBlobClient(parquetName);
            var parquetResult = await parquetBlob.DownloadContentAsync();
            var log = TransactionLogEntry.LoadDeltaLogFromParquet(
                42,
                kustoDatabaseName,
                kustoTableName,
                parquetResult.Value.Content.ToStream());

            throw new NotImplementedException();
        }

        private async Task<TransactionLog> LoadTransactionBlobAsync(
            long txId,
            string blobName,
            string kustoDatabaseName,
            string kustoTableName)
        {
            try
            {
                var blobClient = _blobContainerClient.GetBlockBlobClient(blobName);
                var downloadResult = await blobClient.DownloadContentAsync();
                var blobText = downloadResult.Value.Content.ToString();

                return TransactionLogEntry.LoadDeltaLogFromJson(
                    txId,
                    kustoDatabaseName,
                    kustoTableName,
                    blobText);
            }
            catch (Exception ex)
            {
                throw new MirrorException($"Error loading transaction log '{blobName}'", ex);
            }
        }

        private long? ExtractTransactionId(string blobName)
        {
            if (blobName.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
            {
                var lastSlash = blobName.LastIndexOf('/');
                var lastDot = blobName.LastIndexOf('.');
                var txIdText = blobName.Substring(lastSlash + 1, lastDot - lastSlash - 1);
                long txId;

                if (long.TryParse(txIdText, out txId))
                {
                    return txId;
                }
            }

            return null;
        }
    }
}