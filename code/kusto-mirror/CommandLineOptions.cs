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

        [Option('c', "cluster", Required = false, HelpText = "Cluster Query URL")]
        public string ClusterQueryUrl { get; set; } = string.Empty;
    }
}