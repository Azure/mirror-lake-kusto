namespace KustoMirrorTest.Simple
{
    public class Simple : TestBase
    {
        [Fact]
        public async Task OneLineOneColumn()
        {
            await using (var session = await GetSparkSessionAsync())
            {
                var script = session.GetResource("OneLineOneColumn.py");
                var output = await session.ExecuteSparkCodeAsync(script);
            }
        }
    }
}