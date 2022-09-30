using Azure.Analytics.Synapse.Spark;
using Azure.Analytics.Synapse.Spark.Models;
using Azure.Core;
using Azure.Identity;
using Kusto.Data.Linq;
using MirrorLakeKusto;
using MirrorLakeKusto.Kusto;
using Microsoft.Azure.Management.Kusto;
using Microsoft.Azure.Management.Kusto.Models;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit.Sdk;

namespace MirrorLakeKustoTest
{
    /// <summary>
    /// Spark stuff based on 
    /// https://github.com/Azure/azure-sdk-for-net/blob/Azure.Analytics.Synapse.Spark_1.0.0-preview.8/sdk/synapse/Azure.Analytics.Synapse.Spark/samples/Sample2_ExecuteSparkStatementAsync.md
    /// </summary>
    public abstract partial class TestBase
    {
        #region Inner Types
        protected class SessionHolder : IAsyncDisposable
        {
            private readonly string _testSetId;
            private readonly string _deltaTableFolder;
            private readonly string _kustoTable;
            private readonly Func<string, string> _getResourceFunc;
            private readonly Func<string, string, string, string, Task> _runMirrorAsync;
            private readonly SparkSessionHolder _sparkSessionHolder;
            private readonly DbHolder _dbHolder;

            public SessionHolder(
                string testSetId,
                int testId,
                string deltaTableFolder,
                string kustoTable,
                Func<string, string> getResourceFunc,
                Func<string, string, string, string, Task> runMirrorAsync,
                SparkSessionHolder sparkSessionHolder,
                DbHolder dbHolder)
            {
                _testSetId = testSetId;
                TestId = testId;
                _deltaTableFolder = deltaTableFolder;
                _kustoTable = kustoTable;
                _getResourceFunc = getResourceFunc;
                _sparkSessionHolder = sparkSessionHolder;
                _runMirrorAsync = runMirrorAsync;
                _dbHolder = dbHolder;
            }

            public int TestId { get; }

            public string SynapseRootFolder => $"/automated-tests/{_testSetId}/{TestId.ToString("D4")}";

            public async Task<SparkStatementOutput> ExecuteSparkCodeAsync(string code)
            {
                return await _sparkSessionHolder.ExecuteSparkCodeAsync(code);
            }

            public string GetResource(string resourceName)
            {
                var rawScript = _getResourceFunc(resourceName);
                var script = rawScript.Replace("<ROOT>", SynapseRootFolder);

                return script;
            }

            public async Task RunMirrorAsync()
            {
                var containerUrl = GetEnvironmentVariable("mlkContainerUrl");
                var checkpointBlobUrl = $"{containerUrl}/{SynapseRootFolder}/checkpoint.csv";
                var deltaTableStorageUrl = $"{containerUrl}/{SynapseRootFolder}/{_deltaTableFolder}";

                await _runMirrorAsync(
                    checkpointBlobUrl,
                    deltaTableStorageUrl,
                    _dbHolder.DbName,
                    _kustoTable);
            }

            public async Task<IImmutableList<T>> ExecuteQueryAsync<T>(
                string queryTextAfterTable,
                Func<IDataRecord, T> projection)
            {
                return await _dbHolder.ExecuteQueryAsync(
                    $"{_kustoTable}\n{queryTextAfterTable}",
                    projection);
            }

            async ValueTask IAsyncDisposable.DisposeAsync()
            {
                await (_sparkSessionHolder as IAsyncDisposable).DisposeAsync();
            }
        }

        protected class SparkSessionHolder : IAsyncDisposable
        {
            private readonly SparkSession _sparkSession;
            private readonly Func<Task> _disposeSparkSessionAsyncFunc;
            private readonly Func<string, Task<SparkStatementOutput>> _executeSparkFunc;

            public SparkSessionHolder(
                SparkSession sparkSession,
                Func<Task> disposeSparkSessionAsyncFunc,
                Func<string, Task<SparkStatementOutput>> executeSparkFunc)
            {
                _sparkSession = sparkSession;
                _disposeSparkSessionAsyncFunc = disposeSparkSessionAsyncFunc;
                _executeSparkFunc = executeSparkFunc;
            }

            public async Task<SparkStatementOutput> ExecuteSparkCodeAsync(string code)
            {
                return await _executeSparkFunc(code);
            }

            async ValueTask IAsyncDisposable.DisposeAsync()
            {
                await _disposeSparkSessionAsyncFunc();
            }
        }

        protected class DbHolder : IAsyncDisposable
        {
            private readonly DatabaseGateway _dbGateway;
            private readonly Func<Task> _deleteDbFunc;

            public DbHolder(DatabaseGateway dbGateway, Func<Task> deleteDbFunc)
            {
                _dbGateway = dbGateway;
                _deleteDbFunc = deleteDbFunc;
            }

            public string DbName => _dbGateway.DatabaseName;

            public async Task<IImmutableList<T>> ExecuteQueryAsync<T>(
                string queryText,
                Func<IDataRecord, T> projection)
            {
                return await _dbGateway.ExecuteQueryAsync(
                    queryText,
                    projection,
                    CancellationToken.None);
            }

            async ValueTask IAsyncDisposable.DisposeAsync()
            {
                await _deleteDbFunc();
            }
        }

        private class DbManagement
        {
            private readonly KustoClusterGateway _clusterGateway;
            private readonly Func<Task<string>> _createDbFunc;
            private readonly Func<string, Task> _deleteDbFunc;

            public DbManagement(
                KustoClusterGateway clusterGateway,
                Func<Task<string>> createDbFunc,
                Func<string, Task> deleteDbFunc)
            {
                _clusterGateway = clusterGateway;
                _createDbFunc = createDbFunc;
                _deleteDbFunc = deleteDbFunc;
            }

            public async Task<DbHolder> GetNewDbAsync()
            {
                var dbName = await _createDbFunc();
                var dbHolder = new DbHolder(
                    new DatabaseGateway(_clusterGateway, dbName),
                    async () => await _deleteDbFunc(dbName));

                return dbHolder;
            }
        }

        private class MainSettings
        {
            public IDictionary<string, ProjectSetting>? Profiles { get; set; }

            public IDictionary<string, string> GetEnvironmentVariables()
            {
                if (Profiles == null)
                {
                    throw new InvalidOperationException("'profiles' element isn't present in 'launchSettings.json'");
                }
                if (Profiles.Count == 0)
                {
                    throw new InvalidOperationException(
                        "No profile is configured within 'profiles' element isn't present "
                        + "in 'launchSettings.json'");
                }
                var profile = Profiles.First().Value;

                if (profile.EnvironmentVariables == null)
                {
                    throw new InvalidOperationException("'environmentVariables' element isn't present in 'launchSettings.json'");
                }

                return profile.EnvironmentVariables;
            }
        }

        private class ProjectSetting
        {
            public IDictionary<string, string>? EnvironmentVariables { get; set; }
        }
        #endregion

        private const string SPARK_SESSION_ID_PATH = "SparkSession.txt";
        private const int CREATE_DB_AHEAD_COUNT = 0;

        private readonly static string? _singleExecPath;
        private readonly static Uri _ingestionUri;
        private readonly static SparkSessionClient _sparkSessionClient;
        private readonly static Task<SparkSession> _sparkSessionTask;
        private readonly static ConcurrentQueue<TaskCompletionSource> _sparkSessionQueue =
            new ConcurrentQueue<TaskCompletionSource>();
        private readonly static Task<DbManagement> _dbManagementTask;
        private readonly static string _testSetId;
        private static volatile int _testId = 0;
        private static volatile int _dbId = 0;

        static TestBase()
        {
            var now = DateTime.Now;

            ReadEnvironmentVariables();
            _singleExecPath = Environment.GetEnvironmentVariable("mlkSingleExecPath");
            _ingestionUri = new Uri(GetEnvironmentVariable("mlkIngestionUri"));
            _testSetId = $"{now.ToString("yyyy-MM-dd")}/{now.ToString("HH:mm:ss")}";
            _sparkSessionClient = CreateSparkSessionClient();
            _dbManagementTask = CreateDbManagementAsync();
            _sparkSessionTask = AcquireSparkSessionAsync();
        }

        #region Environment variables
        private static void ReadEnvironmentVariables()
        {
            const string PATH = "Properties\\launchSettings.json";

            if (File.Exists(PATH))
            {
                var settingContent = File.ReadAllText(PATH);
                var mainSetting = JsonSerializer.Deserialize<MainSettings>(
                    settingContent,
                    new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    });

                if (mainSetting == null)
                {
                    throw new InvalidOperationException("Can't read 'launchSettings.json'");
                }

                var variables = mainSetting.GetEnvironmentVariables();

                foreach (var variable in variables)
                {
                    Environment.SetEnvironmentVariable(variable.Key, variable.Value);
                }
            }
        }

        private static string GetEnvironmentVariable(string variableName)
        {
            var value = Environment.GetEnvironmentVariable(variableName);

            if (value == null)
            {
                throw new InvalidOperationException(
                    $"Environment variable '{variableName}' missing");
            }

            return value;
        }
        #endregion

        #region Azure
        private static ClientSecretCredential CreateAzureCredentials()
        {
            var tenantId = GetEnvironmentVariable("mlkTenantId");
            var appId = GetEnvironmentVariable("mlkSpId");
            var appSecret = GetEnvironmentVariable("mlkSpSecret");
            var credential = new ClientSecretCredential(tenantId, appId, appSecret);

            return credential;
        }
        #endregion

        #region Session
        protected async Task<SessionHolder> GetTestSessionAsync(
            string deltaTableFolder,
            string kustoTable)
        {
            var sparkSessionTask = GetSparkSessionAsync();
            var db = await GetNewDbAsync();
            var sparkSession = await sparkSessionTask;

            return new SessionHolder(
                _testSetId,
                Interlocked.Increment(ref _testId),
                deltaTableFolder,
                kustoTable,
                GetResource,
                RunMirrorAsync,
                sparkSession,
                db);
        }

        private async Task RunMirrorAsync(
            string checkpointBlobUrl,
            string deltaTableStorageUrl,
            string database,
            string kustoTable)
        {
            var ingestionConnectionString = GetIngestionConnectionString();
            var args = new[]
            {
                "-s",
                deltaTableStorageUrl,
                "-c",
                checkpointBlobUrl,
                "-i",
                ingestionConnectionString,
                "-d",
                database,
                "-t",
                kustoTable
            };

            Console.WriteLine($"> mirror-lake-kusto {string.Join(' ', args)}");
            if (string.IsNullOrEmpty(_singleExecPath))
            {
                await Program.Main(args);
            }
            else
            {
                throw new NotImplementedException("Out-of-proc");
            }
        }

        private static string GetIngestionConnectionString()
        {
            var tenantId = GetEnvironmentVariable("mlkTenantId");
            var appId = GetEnvironmentVariable("mlkSpId");
            var appSecret = GetEnvironmentVariable("mlkSpSecret");
            var ingestionConnectionString = $"Data Source={_ingestionUri};"
                + $"Application Client Id={appId};Application Key={appSecret};"
                + $"Authority Id={tenantId}";
            return ingestionConnectionString;
        }
        #endregion

        #region Spark
        private async Task<SparkSessionHolder> GetSparkSessionAsync()
        {
            var sparkSession = await _sparkSessionTask;
            var waitingSource = new TaskCompletionSource();

            //  Algorithm for the queue:
            //  1- Each requester enqueue their own source (as we do here)
            //  2- Requester wait for their task to unlock
            //  3- When they are done, they dequeue themselves
            //  4- After dequeuing themselves, they unlock the next one
            //  2b- If they are actually at the top of the queue it means they are alone or are about to be unlocked

            //  Step 1
            _sparkSessionQueue.Enqueue(waitingSource);

            TaskCompletionSource? currentTop;

            if (_sparkSessionQueue.TryPeek(out currentTop))
            {
                if (currentTop.Task.Id == waitingSource.Task.Id)
                {   //  Step 2b (requester on top of queue already)
                    //  We unlock ourselves in case we are alone
                    waitingSource.SetResult();
                }
                //  Step 2
                await waitingSource.Task;

                //  Clean variables in the Spark session (cheap way to recycle sessions)
                //await CleanSparkSessionAsync(sparkSession);

                return new SparkSessionHolder(
                    sparkSession,
                    async () =>
                    {
                        //  This isn't asynchronous
                        await Task.CompletedTask;

                        //  Step 3
                        if (_sparkSessionQueue.TryDequeue(out currentTop))
                        {
                            if (currentTop.Task.Id == waitingSource.Task.Id)
                            {
                                if (_sparkSessionQueue.TryPeek(out currentTop))
                                {   //  Step 4
                                    currentTop.SetResult();
                                }
                                else
                                {   //  No more requester in queue (that's fine)
                                }
                            }
                            else
                            {
                                throw new InvalidOperationException("No more Spark requester?");
                            }
                        }
                    },
                    async (code) => await ExecuteSparkCodeAsync(sparkSession, code));
            }
            else
            {   //  Nobody in the queue, impossible since requester unqueue themselves
                throw new InvalidOperationException("Spark Session queue empty");
            }
        }

        private static async Task<SparkStatementOutput> ExecuteSparkCodeAsync(
            SparkSession sparkSession,
            string code)
        {
            var sparkStatementRequest = new SparkStatementOptions
            {
                Kind = SparkStatementLanguageType.PySpark,
                Code = code
            };
            var createStatementOperation = await _sparkSessionClient.StartCreateSparkStatementAsync(
                sparkSession.Id,
                sparkStatementRequest);
            var statementCreated = await createStatementOperation.WaitForCompletionAsync();

            if (statementCreated.Value.State != LivyStatementStates.Available)
            {
                throw new InvalidOperationException(
                    "Spark session command returned in a bad state of "
                    + $"'{statementCreated.Value.State}'");
            }
            if (statementCreated.Value.Output.ErrorValue != null)
            {
                throw new InvalidOperationException(
                    "Spark session command returned an error name of"
                    + $"'{statementCreated.Value.Output.ErrorName}'"
                    + $"and an error value of '{statementCreated.Value.Output.ErrorValue}'");
            }

            return statementCreated.Value.Output;
        }

        private static async Task CleanSparkSessionAsync(SparkSession sparkSession)
        {
            await ExecuteSparkCodeAsync(sparkSession, "%reset -f");
        }

        private static async Task<SparkSession> AcquireSparkSessionAsync()
        {
            var session = await RetrieveSparkSessionAsync();

            if (session == null)
            {
                session = await CreateSparkSessionAsync();
                await PersistSessionSparkAsync(session);
            }

            return session;
        }

        private static async Task PersistSessionSparkAsync(SparkSession session)
        {
            var content = session.Id.ToString();

            await File.WriteAllTextAsync(SPARK_SESSION_ID_PATH, content);
        }

        private static async Task<SparkSession?> RetrieveSparkSessionAsync()
        {
            if (File.Exists(SPARK_SESSION_ID_PATH))
            {
                try
                {
                    var content = await File.ReadAllTextAsync(SPARK_SESSION_ID_PATH);
                    int id;

                    if (int.TryParse(content, out id))
                    {
                        var session = await _sparkSessionClient.GetSparkSessionAsync(id);

                        if (session.Value.State == LivyStates.Dead)
                        {
                            return null;
                        }
                        else
                        {
                            return session.Value;
                        }
                    }
                }
                catch
                {   //  Can't retrieve spark session id or session
                }
            }

            return null;
        }

        private static async Task<SparkSession> CreateSparkSessionAsync()
        {
            var request = new SparkSessionOptions(name: $"session-{Guid.NewGuid()}")
            {
                DriverMemory = "28g",
                DriverCores = 4,
                ExecutorMemory = "28g",
                ExecutorCores = 4,
                ExecutorCount = 2
            };
            var createSessionOperation = await _sparkSessionClient.StartCreateSparkSessionAsync(request);
            var sessionCreated = await createSessionOperation.WaitForCompletionAsync();
            var session = sessionCreated.Value;

            return session;
        }

        private static SparkSessionClient CreateSparkSessionClient()
        {
            var sparkPoolName = GetEnvironmentVariable("mlkSparkPoolName");
            var endpoint = GetEnvironmentVariable("mlkSparkEndpoint");
            var credential = CreateAzureCredentials();
            var client = new SparkSessionClient(new Uri(endpoint), sparkPoolName, credential);

            return client;
        }
        #endregion

        #region Embedded Resource
        private string GetResource(string resourceName)
        {
            var assembly = this.GetType().GetTypeInfo().Assembly;
            var typeNamespace = this.GetType().Namespace;
            var fullResourceName = $"{typeNamespace}.Scripts.{resourceName}";

            using (var stream = assembly.GetManifestResourceStream(fullResourceName))
            {
                if (stream == null)
                {
                    throw new ArgumentException(
                        $"Can't find resource file '{resourceName}'",
                        nameof(resourceName));
                }
                using (var reader = new StreamReader(stream))
                {
                    var text = reader.ReadToEnd();

                    return text;
                }
            }
        }
        #endregion

        #region Kusto
        private static async Task<DbManagement> CreateDbManagementAsync()
        {
            var tenantId = GetEnvironmentVariable("mlkTenantId");
            var appId = GetEnvironmentVariable("mlkSpId");
            var appSecret = GetEnvironmentVariable("mlkSpSecret");
            var subscriptionId = GetEnvironmentVariable("mlkSubscriptionId");
            var resourceGroup = GetEnvironmentVariable("mlkResourceGroup");
            var clusterName = GetEnvironmentVariable("mlkCluster");
            var dbPrefix = GetEnvironmentVariable("mlkDbPrefix");
            var ingestionConnectionString = GetIngestionConnectionString();
            var credential = new ClientCredential(appId, appSecret);
            var authenticationContext = new AuthenticationContext($"https://login.windows.net/{tenantId}");
            var result = await authenticationContext.AcquireTokenAsync(
                "https://management.core.windows.net/",
                credential);
            var tokenCredentials = new TokenCredentials(result.AccessToken, result.AccessTokenType);
            var kustoManagementClient = new KustoManagementClient(tokenCredentials)
            {
                SubscriptionId = subscriptionId
            };
            var databases = kustoManagementClient.Databases;
            var dbList = await databases.ListByClusterAsync(resourceGroup, clusterName);
            var dbsDeletionTasks = dbList
                .Select(db => db.Name.Split('/').Last())
                .Where(dbName => dbName.StartsWith(dbPrefix))
                .Select(dbName => databases.DeleteAsync(resourceGroup, clusterName, dbName))
                .ToImmutableArray();
            var clusterTask = kustoManagementClient.Clusters.GetAsync(resourceGroup, clusterName);
            var cluster = await clusterTask;
            var dbCreationTaskQueue = new ConcurrentQueue<Task<string>>();
            var clusterGateway = await KustoClusterGateway.CreateAsync(
                ingestionConnectionString,
                string.Empty);

            await Task.WhenAll(dbsDeletionTasks);

            var dbManagement = new DbManagement(
                clusterGateway,
                async () =>
                {
                    Func<Task<string>> dbCreationFunc = async () =>
                    {
                        var dbName = $"{dbPrefix}_{Interlocked.Increment(ref _dbId)}";
                        var db = new ReadWriteDatabase
                        {
                            Location = cluster.Location
                        };

                        await databases.CreateOrUpdateAsync(resourceGroup, clusterName, dbName, db);

                        return dbName;
                    };
                    Task<string>? dbCreationTask;

                    dbCreationTaskQueue.TryDequeue(out dbCreationTask);
                    if (dbCreationTask == null)
                    {
                        dbCreationTask = dbCreationFunc();
                    }
                    //  Fill ahead for better performance of later calls
                    while (dbCreationTaskQueue.Count < CREATE_DB_AHEAD_COUNT)
                    {
                        dbCreationTaskQueue.Enqueue(dbCreationFunc());
                    }

                    return await dbCreationTask;
                },
                async (dbName) => await databases.DeleteAsync(resourceGroup, clusterName, dbName));

            return dbManagement;
        }

        protected async Task<DbHolder> GetNewDbAsync()
        {
            var dbManagement = await _dbManagementTask;

            return await dbManagement.GetNewDbAsync();
        }
        #endregion
    }
}