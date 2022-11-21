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
            IEnumerable<DeltaTableParameterization> deltaTableParameterizations,
            bool forceBrowserAuth)
        {
            ContinuousRun = continuousRun;
            ClusterQueryConnectionString = clusterIngestionConnectionString;
            CheckpointBlobFolderUrl = checkpointBlobFolderUrl;
            DeltaTableParameterizations = deltaTableParameterizations.ToImmutableArray();
            ForceBrowserAuth = forceBrowserAuth;
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
                options.CreationTime,
                GetDate(options.GoBack));

            return new MainParameterization(
                options.ContinuousRun,
                options.ClusterQueryConnectionString,
                checkpointBlobFolderUrl,
                new[] { deltaTable },
                options.ForceBrowserAuth);
        }

        private static DateTime? GetDate(string? goBack)
        {
            if (string.IsNullOrWhiteSpace(goBack))
            {
                return null;
            }
            else
            {
                var segments = goBack.Split(new[] { '-', '/' });

                if (segments.Length != 3)
                {
                    throw new MirrorException(
                        $"Go back date '{goBack}' should have 3 segments separated by '-' or '/'");
                }

                var numbers = segments
                    .Select(s => int.TryParse(s, out var n) ? (int?)n : null)
                    .ToImmutableArray();

                if (numbers.Any(n => n == null))
                {
                    throw new MirrorException(
                        $"Go back date '{goBack}' have segments that aren't numbers");
                }

                var day = numbers[0]!.Value;
                var month = numbers[1]!.Value;
                var year = numbers[2]!.Value;

                try
                {
                    var date = new DateTime(year, month, day);

                    return date;
                }
                catch (ArgumentOutOfRangeException ex)
                {
                    throw new MirrorException(
                        $"Go back date '{goBack}' isn't valid date",
                        ex);
                }
            }
        }

        public bool ContinuousRun { get; }

        public bool ForceBrowserAuth { get; }

        public string ClusterQueryConnectionString { get; }

        public Uri CheckpointBlobFolderUrl { get; }

        public IImmutableList<DeltaTableParameterization> DeltaTableParameterizations { get; }
    }
}