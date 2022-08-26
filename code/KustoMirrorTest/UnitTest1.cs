namespace KustoMirrorTest
{
    public class UnitTest1 : TestBase
    {
        [Fact]
        public async Task Test1Async()
        {
            var session = await GetSparkSessionAsync();
        }
    }
}