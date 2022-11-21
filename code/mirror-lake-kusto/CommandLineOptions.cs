using CommandLine;

namespace MirrorLakeKusto
{
    internal class CommandLineOptions
    {
        [Option('v', "verbose", Required = false, HelpText = "Set output to verbose messages.")]
        public bool Verbose { get; set; } = false;

        [Option(
            "continuous",
            Required = false,
            HelpText = "Continuous run:  if set, runs continuously, otherwise, stop after first batch")]
        public bool ContinuousRun { get; set; } = false;

        [Option(
            "creation-time",
            Required = false,
            HelpText = "Kusto expression to resolve into extent creation time")]
        public string? CreationTime { get; set; }

        [Option('c', "checkpoint", Required = false, HelpText = "Checkpoint CSV blob folder URL")]
        public string CheckpointBlobFolderUrl { get; set; } = string.Empty;

        [Option('s', "storage", Required = false, HelpText = "Delta Table Storage URL")]
        public string DeltaTableStorageUrl { get; set; } = string.Empty;

        [Option(
            'k',
            "ingestion",
            Required = false,
            HelpText = "Cluster Query Connection string (cf https://docs.microsoft.com/en-us/azure/data-explorer/kusto/api/connection-strings/kusto)")]
        public string ClusterQueryConnectionString { get; set; } = string.Empty;

        [Option('d', "db", Required = false, HelpText = "Kusto Database")]
        public string Database { get; set; } = string.Empty;

        [Option('t', "table", Required = false, HelpText = "Kusto Table")]
        public string KustoTable { get; set; } = string.Empty;

        [Option('g', "go-back", Required = false, HelpText = "What point in time to go back")]
        public string? GoBack { get; set; }

        [Option("force-browser-auth", Required = false, HelpText = "Forces browser authentication on all")]
        public bool ForceBrowserAuth { get; set; } = false;
    }
}