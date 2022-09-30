using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MirrorLakeKusto.Parameters
{
    internal class MainParameterization
    {
        public MainParameterization(
            bool continuousRun,
            string clusterIngestionConnectionString,
            Uri checkpointBlobUrl,
            IEnumerable<DeltaTableParameterization> deltaTableParameterizations)
        {
            ContinuousRun = continuousRun;
            ClusterIngestionConnectionString = clusterIngestionConnectionString;
            CheckpointBlobUrl = checkpointBlobUrl;
            DeltaTableParameterizations = deltaTableParameterizations.ToImmutableArray();
        }

        public static MainParameterization Create(CommandLineOptions options)
        {
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
                options.ContinuousRun,
                options.ClusterIngestionConnectionString,
                checkpointBlobUrl,
                new[] { deltaTable });
        }

        public bool ContinuousRun { get; }
        
        public string ClusterIngestionConnectionString { get; }

        public Uri CheckpointBlobUrl { get; }

        public IImmutableList<DeltaTableParameterization> DeltaTableParameterizations { get; }
    }
}