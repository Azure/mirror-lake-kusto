using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kusto.Mirror.ConsoleApp.Parameters
{
    internal class MainParameterization
    {
        public MainParameterization(
            AuthenticationMode authenticationMode,
            Uri clusterQueryUri,
            Uri checkpointBlobUrl,
            IEnumerable<DeltaTableParameterization> deltaTableParameterizations)
        {
            AuthenticationMode = authenticationMode;
            ClusterIngestionUri = clusterQueryUri;
            CheckpointBlobUrl = checkpointBlobUrl;
            DeltaTableParameterizations = deltaTableParameterizations.ToImmutableArray();
        }

        public static MainParameterization Create(CommandLineOptions options)
        {
            Uri? clusterQueryUri;

            if (Uri.TryCreate(options.ClusterIngestionUrl, UriKind.Absolute, out clusterQueryUri))
            {
                if (!string.IsNullOrWhiteSpace(clusterQueryUri.Query))
                {
                    throw new MirrorException(
                        $"Cluster query URL can't contain query string:  "
                        + $"'{options.ClusterIngestionUrl}'");
                }
            }
            else
            {
                throw new MirrorException(
                    $"Invalid cluster query URL:  '{options.ClusterIngestionUrl}'");
            }

            Uri? checkpointBlobUrl;
            Uri? deltaTableStorageUrl;

            if (!Uri.TryCreate(options.CheckpointBlobUrl, UriKind.Absolute, out checkpointBlobUrl))
            {
                throw new MirrorException(
                    $"Invalid Checkpoint Blob URL:  '{options.CheckpointBlobUrl}'");
            }
            if (!Uri.TryCreate(options.DeltaTableStorageUrl, UriKind.Absolute, out deltaTableStorageUrl))
            {
                throw new MirrorException(
                    $"Invalid Delta Table URL:  '{options.DeltaTableStorageUrl}'");
            }

            var deltaTable = new DeltaTableParameterization(
                deltaTableStorageUrl,
                options.Database,
                options.KustoTable,
                options.IngestPartitionColumns);

            return new MainParameterization(
                options.AuthenticationMode,
                clusterQueryUri,
                checkpointBlobUrl,
                new[] { deltaTable });
        }

        public AuthenticationMode AuthenticationMode { get; }

        public Uri ClusterIngestionUri { get; }

        public Uri CheckpointBlobUrl { get; }

        public IImmutableList<DeltaTableParameterization> DeltaTableParameterizations { get; }
    }
}