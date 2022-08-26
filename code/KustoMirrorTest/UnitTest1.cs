namespace KustoMirrorTest
{
    public class UnitTest1 : TestBase
    {
        [Fact]
        public async Task Test1Async()
        {
            //  https://github.com/Azure/azure-sdk-for-net/blob/Azure.Analytics.Synapse.Spark_1.0.0-preview.8/sdk/synapse/Azure.Analytics.Synapse.Spark/samples/Sample2_ExecuteSparkStatementAsync.md
            var session = await CreateSparkSessionAsync();
        }
    }
}