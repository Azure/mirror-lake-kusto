using Azure;
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
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace MirrorLakeKusto.Storage.DeltaLake
{
    internal class DeltaTableGateway
    {
        #region Inner Types
        internal class Checkpoint
        {
            public long Version { get; set; }

            public long Size { get; set; }
        }
        #endregion

        private readonly static DataTableGatewaySerializerContext _dataTableGatewaySerializerContext =
            new DataTableGatewaySerializerContext(
                new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                });

        private const string TX_ID_FORMAT = "00000000000000000000";
        private const string TX_ID_EXTENSION = ".json";

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

        internal async Task<TransactionLog?> GetNextTransactionLogAsync(
            TransactionLog? currentLog,
            string kustoDatabaseName,
            string kustoTableName,
            CancellationToken ct)
        {
            var lastCheckpointName = $"{_transactionFolderPrefix}/_last_checkpoint";
            var lastCheckpointBlob = _blobContainerClient.GetBlobClient(lastCheckpointName);
            var lastCheckpointExists = await lastCheckpointBlob.ExistsAsync();

            //  Use checkpoint
            if (lastCheckpointExists.Value)
            {
                var checkpointTxId = await ReadCheckpointTxIdAsync(lastCheckpointBlob);

                if (currentLog == null || currentLog.EndTxId + 1 < checkpointTxId)
                {   //  Complete checkpoint with logs after checkpoint
                    var afterLogTask = GetTransactionLogFromAsync(
                        checkpointTxId + 1,
                        checkpointTxId + 10,
                        kustoDatabaseName,
                        kustoTableName,
                        ct);
                    var cummulativeTxLog = await ExtractCheckpointTxLogAsync(
                        checkpointTxId,
                        kustoDatabaseName,
                        kustoTableName);
                    var deltaLog = currentLog == null
                        ? cummulativeTxLog
                        : cummulativeTxLog.Delta(currentLog);
                    var afterLog = await afterLogTask;
                    var nextLog = afterLog == null
                        ? deltaLog
                        : deltaLog.Coalesce(afterLog);

                    return nextLog;
                }
                else
                {   //  Checkpoint isn't useful, just read the logs
                    return await GetTransactionLogFromAsync(
                        currentLog == null ? 0 : currentLog.EndTxId + 1,
                        checkpointTxId + 10,
                        kustoDatabaseName,
                        kustoTableName,
                        ct);
                }
            }
            else
            {   //  No checkpoint, just read the logs
                return await GetTransactionLogFromAsync(
                    currentLog == null ? null : currentLog.EndTxId + 1,
                    currentLog == null ? null : 9,
                    kustoDatabaseName,
                    kustoTableName,
                    ct);
            }
        }

        private async Task<TransactionLog?> GetTransactionLogFromAsync(
            long? fromTxId,
            long? toTxId,
            string kustoDatabaseName,
            string kustoTableName,
            CancellationToken ct)
        {
            var blobItems = await GetTransactionBlobItemsAsync(fromTxId, toTxId, ct);
            var txIds = blobItems
                .Select(b => ExtractTransactionIdName(b.Name))
                .Where(n => n != null)
                .Select(n => long.Parse(n!))
                .Where(txId => fromTxId == null || txId >= fromTxId);
            var txLogTasks = txIds
                .OrderBy(txId => txId)
                .Select(txId => LoadTransactionBlobAsync(
                    txId,
                    GetTransactionBlobPath(txId),
                    kustoDatabaseName,
                    kustoTableName,
                    ct))
                .ToImmutableArray();

            await Task.WhenAll(txLogTasks);

            var txLogs = txLogTasks
                .Select(t => t.Result)
                .ToImmutableArray();

            if (txLogs.Any())
            {
                return TransactionLog.Coalesce(txLogs);
            }
            else
            {
                return null;
            }
        }

        private async Task<IEnumerable<BlobItem>> GetTransactionBlobItemsAsync(
            long? fromTxId,
            long? toTxId,
            CancellationToken ct)
        {
            if (fromTxId == null)
            {
                if (toTxId != null)
                {
                    throw new ArgumentException("Should be null at this point", nameof(toTxId));
                }
                //  Take all blob items
                var blobPageable = _blobContainerClient.GetBlobsAsync(
                    BlobTraits.None,
                    BlobStates.None,
                    _transactionFolderPrefix,
                    ct);
                var blobItems = await blobPageable.ToListAsync();

                return blobItems;
            }
            else
            {
                if (toTxId == null)
                {
                    throw new ArgumentException("Should NOT be null at this point", nameof(toTxId));
                }

                var prefixes = Enumerable.Range(0, (int)(toTxId - fromTxId) + 1)
                    .Select(i => i + fromTxId.Value)
                    .Select(txId => GetTransactionBlobPath(txId))
                    .Select(path => path.Substring(0, path.LastIndexOf('.') - 1))
                    .Distinct();
                //  Take all blob items
                var blobItemsTasks = prefixes
                    .Select(prefix => _blobContainerClient.GetBlobsAsync(
                        BlobTraits.None,
                        BlobStates.None,
                        prefix,
                        ct))
                    .Select(pageable => pageable.ToListAsync())
                    .ToImmutableArray();

                await Task.WhenAll(blobItemsTasks);

                var items = blobItemsTasks
                    .Select(t => t.Result)
                    .SelectMany(items => items)
                    .ToImmutableArray();

                return items;
            }
        }

        private string GetTransactionBlobPath(long txId)
        {
            var paddedTxId = txId.ToString(TX_ID_FORMAT);
            var blobName = $"{_transactionFolderPrefix}//{paddedTxId}{TX_ID_EXTENSION}";

            return blobName;
        }

        private async Task<TransactionLog> LoadTransactionBlobAsync(
            long txId,
            string blobName,
            string kustoDatabaseName,
            string kustoTableName,
            CancellationToken ct)
        {
            try
            {
                var blobClient = _blobContainerClient.GetBlockBlobClient(blobName);
                var downloadResult = await blobClient.DownloadContentAsync(ct);
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

        private string? ExtractTransactionIdName(string blobName)
        {
            if (blobName.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
            {
                var lastSlash = blobName.LastIndexOf('/');
                var lastDot = blobName.LastIndexOf('.');
                var txIdText = blobName.Substring(lastSlash + 1, lastDot - lastSlash - 1);

                if (long.TryParse(txIdText, out _))
                {
                    return txIdText;
                }
            }

            return null;
        }

        private async Task<long> ReadCheckpointTxIdAsync(BlobClient lastCheckpointBlob)
        {
            var checkpointResult = await lastCheckpointBlob.DownloadContentAsync();
            var checkpointText = checkpointResult.Value.Content.ToString();
            var checkpointObject = JsonSerializer.Deserialize(
                checkpointText,
                typeof(Checkpoint),
                _dataTableGatewaySerializerContext);

            if (checkpointObject == null)
            {
                throw new InvalidOperationException($"Could deserialize checkpoint object:  '{checkpointText}'");
            }
            else
            {
                var checkpoint = (Checkpoint)checkpointObject;

                return checkpoint.Version;
            }
        }

        private async Task<TransactionLog> ExtractCheckpointTxLogAsync(
            long checkpointTxId,
            string kustoDatabaseName,
            string kustoTableName)
        {
            var paddedVersion = checkpointTxId.ToString("D20");
            var parquetName = $"{_transactionFolderPrefix}/{paddedVersion}.checkpoint.parquet";
            var parquetBlob = _blobContainerClient.GetBlobClient(parquetName);
            var parquetResult = await parquetBlob.DownloadContentAsync();
            var log = TransactionLogEntry.LoadDeltaLogFromParquet(
                checkpointTxId,
                kustoDatabaseName,
                kustoTableName,
                parquetResult.Value.Content.ToStream());

            return log;
        }
    }

    [JsonSerializable(typeof(DeltaTableGateway.Checkpoint))]
    internal partial class DataTableGatewaySerializerContext : JsonSerializerContext
    {
    }
}