using Azure.Core;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MirrorLakeKusto.Storage
{
    internal class CheckpointGateway
    {
        private readonly AppendBlobClient _blobClient;

        public CheckpointGateway(Uri blobUri, TokenCredential credential)
        {
            var builder = new BlobUriBuilder(blobUri);

            //  Enforce blob storage API
            builder.Host =
                builder.Host.Replace(".dfs.core.windows.net", ".blob.core.windows.net");
            blobUri = builder.ToUri();

            _blobClient = new AppendBlobClient(blobUri, credential);
        }

        public async Task<bool> ExistsAsync(CancellationToken ct)
        {
            return await _blobClient.ExistsAsync(ct);
        }

        public async Task CreateAsync(CancellationToken ct)
        {
            await _blobClient.CreateAsync(new AppendBlobCreateOptions(), ct);
        }

        public async Task<byte[]> ReadAllContentAsync(CancellationToken ct)
        {
            var result = await _blobClient.DownloadContentAsync(ct);
            var buffer = result.Value.Content.ToArray();

            return buffer;
        }

        public async Task WriteAsync(byte[] buffer, CancellationToken ct)
        {
            using (var stream = new MemoryStream(buffer))
            {
                await _blobClient.AppendBlockAsync(stream, cancellationToken: ct);
            }
        }
    }
}