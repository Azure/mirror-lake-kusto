using CommandLine;

namespace Kusto.Mirror.ConsoleApp
{
    internal class CommandLineOptions
    {
        [Option('v', "verbose", Required = false, HelpText = "Set output to verbose messages.")]
        public bool Verbose { get; set; }

        [Option('a', "authMode", Required = false, HelpText = "Authentication mode:  AppSecret, AzCli or Browser")]
        public AuthenticationMode AuthenticationMode { get; set; } = AuthenticationMode.AzCli;

        [Option('d', "delta", Required = false, HelpText = "Delta Table URL")]
        public string DeltaTableUrl { get; set; } = string.Empty;

        [Option('i', "ingestion", Required = false, HelpText = "Cluster Ingestion URL")]
        public string ClusterIngestionUrl { get; set; } = string.Empty;

        [Option("db", Required = false, HelpText = "Kusto Database")]
        public string Database { get; set; } = string.Empty;

        [Option('t', "table", Required = false, HelpText = "Kusto Table")]
        public string KustoTable { get; set; } = string.Empty;

        [Option(
            'p',
            "partition",
            Required = false,
            HelpText = "Ingest Partition Columns (true / false)")]
        public bool IngestPartitionColumns { get; set; } = true;
    }
}