using CommandLine;
using CommandLine.Text;
using MirrorLakeKusto.Parameters;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace MirrorLakeKusto
{
    public class Program
    {
        #region Inner Types
        private class MultiFilter : TraceFilter
        {
            private readonly IImmutableList<TraceFilter> _filters;

            public MultiFilter(params TraceFilter[] filters)
            {
                _filters = filters.ToImmutableArray();
            }

            public override bool ShouldTrace(
                TraceEventCache? cache,
                string source,
                TraceEventType eventType,
                int id,
                string? formatOrMessage,
                object?[]? args,
                object? data1,
                object?[]? data)
            {
                foreach (var filter in _filters)
                {
                    if (!filter.ShouldTrace(
                        cache,
                        source,
                        eventType,
                        id,
                        formatOrMessage,
                        args,
                        data1,
                        data))
                    {
                        return false;
                    }
                }

                return true;
            }
        }
        #endregion

        public static string AssemblyVersion
        {
            get
            {
                var versionAttribute = typeof(Program)
                    .Assembly
                    .GetCustomAttribute<AssemblyInformationalVersionAttribute>();
                var version = versionAttribute == null
                    ? "<VERSION MISSING>"
                    : versionAttribute!.InformationalVersion;

                return version;
            }
        }

        public static async Task<int> Main(string[] args)
        {
            var sessionId = Guid.NewGuid().ToString();

            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
            CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo("en-US");

            Console.WriteLine();
            Console.WriteLine($"kusto-mirror {AssemblyVersion}");
            Console.WriteLine($"Session:  {sessionId}");
            Console.WriteLine();

            //  Use CommandLineParser NuGet package to parse command line
            //  See https://github.com/commandlineparser/commandline
            var parser = new Parser(with =>
            {
                with.HelpWriter = null;
            });

            try
            {
                var result = parser.ParseArguments<CommandLineOptions>(args);

                await result
                    .WithNotParsed(errors => HandleParseError(result, errors))
                    .WithParsedAsync(async options => await RunOptionsAsync(options, sessionId));

                return result.Tag == ParserResultType.Parsed
                    ? 0
                    : 1;
            }
            catch (MirrorException ex)
            {
                Trace.TraceError("Encountered error");
                DisplayMirrorException(ex);

                return 1;
            }
            catch (Exception ex)
            {
                Trace.TraceError("Encountered error");
                DisplayGenericException(ex);

                return 1;
            }
            finally
            {
                Console.Out.Flush();
            }
        }

        private static void HandleParseError(
            ParserResult<CommandLineOptions> result,
            IEnumerable<Error> errors)
        {
            var helpText = HelpText.AutoBuild(result, h =>
            {
                h.AutoVersion = false;
                h.Copyright = string.Empty;
                h.Heading = string.Empty;

                return HelpText.DefaultParsingErrorsHandler(result, h);
            }, example => example);

            Console.WriteLine(helpText);
        }

        private static void DisplayMirrorException(MirrorException ex, string tab = "")
        {
            Trace.TraceError($"{tab}Error:  {ex.Message}");

            var copyInnerException = ex.InnerException as MirrorException;

            if (copyInnerException != null)
            {
                DisplayMirrorException(copyInnerException, tab + "  ");
            }
            if (ex.InnerException != null)
            {
                DisplayGenericException(ex.InnerException, tab + "  ");
            }
        }

        private static void DisplayGenericException(Exception ex, string tab = "")
        {
            Console.Error.WriteLine(
                $"{tab}Exception encountered:  {ex.GetType().FullName} ; {ex.Message}");
            Console.Error.WriteLine($"{tab}Stack trace:  {ex.StackTrace}");
            if (ex.InnerException != null)
            {
                DisplayGenericException(ex.InnerException, tab + "  ");
            }
        }

        private static async Task RunOptionsAsync(CommandLineOptions options, string sessionId)
        {
            ConfigureTrace(options.Verbose);
            Trace.TraceInformation("");
            Trace.WriteLine("Initialization...");

            var parameters = MainParameterization.Create(options);
            var requestDescription = CreateRequestDescription(parameters, sessionId);
            var cancellationTokenSource = new CancellationTokenSource();
            var taskCompletionSource = new TaskCompletionSource();

            AppDomain.CurrentDomain.ProcessExit += (e, s) =>
            {
                Trace.TraceInformation("Exiting process...");
                cancellationTokenSource.Cancel();
                taskCompletionSource.Task.Wait();
            };

            try
            {
                await using (var orchestration = await MirrorOrchestration.CreationOrchestrationAsync(
                    parameters,
                    AssemblyVersion,
                    requestDescription,
                    cancellationTokenSource.Token))
                {
                    Trace.WriteLine("Start mirroring...");

                    await orchestration.RunAsync(cancellationTokenSource.Token);
                }

            }
            finally
            {
                taskCompletionSource.SetResult();
            }
        }

        private static string? CreateRequestDescription(
            MainParameterization parameters,
            string sessionId)
        {
            if (Environment.GetEnvironmentVariable("kusto-mirror-automated-tests") != "true")
            {
                var description = new RequestDescription
                {
                    SessionId = sessionId,
                    Os = Environment.OSVersion.Platform.ToString(),
                    OsVersion = Environment.OSVersion.VersionString,
                    Tables = parameters.DeltaTableParameterizations.Select(j => new RequestDescriptionTable
                    {
                        IngestPartitionColumns = j.IngestPartitionColumns
                    }).ToList()
                };
                var buffer = JsonSerializer.SerializeToUtf8Bytes(
                    description,
                    typeof(RequestDescription),
                    new RequestDescriptionSerializerContext());
                var jsonDescription = UTF8Encoding.ASCII.GetString(buffer);

                return jsonDescription;
            }
            else
            {
                return null;
            }
        }

        private static void ConfigureTrace(bool isVerbose)
        {
            var consoleListener = new TextWriterTraceListener(Console.Out)
            {
                Filter = new MultiFilter(
                    new EventTypeFilter(isVerbose ? SourceLevels.Verbose : SourceLevels.Information),
                    new SourceFilter("kusto-mirror"))
            };
            var errorListener = new TextWriterTraceListener(Console.Error)
            {
                Filter = new EventTypeFilter(SourceLevels.Error)
            };

            Trace.Listeners.Add(consoleListener);
            Trace.Listeners.Add(errorListener);
            if (isVerbose)
            {
                Trace.TraceInformation("Verbose output enabled");
            }
        }
    }
}