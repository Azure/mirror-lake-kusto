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
            Uri checkpointBlobFolderUrl,
            IEnumerable<DeltaTableParameterization> deltaTableParameterizations)
        {
            ContinuousRun = continuousRun;
            ClusterIngestionConnectionString = clusterIngestionConnectionString;
            CheckpointBlobFolderUrl = checkpointBlobFolderUrl;
            DeltaTableParameterizations = deltaTableParameterizations.ToImmutableArray();
        }

        public static MainParameterization Create(CommandLineOptions options)
        {
            Uri? checkpointBlobFolderUrl;
            Uri? deltaTableStorageUrl;

            if (!Uri.TryCreate(
                options.CheckpointBlobFolderUrl,
                UriKind.Absolute,
                out checkpointBlobFolderUrl))
            {
                throw new MirrorException(
                    $"Invalid Checkpoint Blob folder URL:  '{options.CheckpointBlobFolderUrl}'");
            }
            if (!Uri.TryCreate(
                options.DeltaTableStorageUrl,
                UriKind.Absolute,
                out deltaTableStorageUrl))
            {
                throw new MirrorException(
                    $"Invalid Delta Table URL:  '{options.DeltaTableStorageUrl}'");
            }

            var deltaTable = new DeltaTableParameterization(
                deltaTableStorageUrl,
                options.Database,
                options.KustoTable,
                options.CreationTime);

            return new MainParameterization(
                options.ContinuousRun,
                options.ClusterIngestionConnectionString,
                checkpointBlobFolderUrl,
                new[] { deltaTable });
        }

        public bool ContinuousRun { get; }
        
        public string ClusterIngestionConnectionString { get; }

        public Uri CheckpointBlobFolderUrl { get; }

        public IImmutableList<DeltaTableParameterization> DeltaTableParameterizations { get; }
    }
}