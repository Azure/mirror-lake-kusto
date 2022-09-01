namespace KustoMirrorTest.Simple
{
    public class Simple : TestBase
    {
        [Fact]
        public async Task OneLineOneColumn()
        {
            await using (var session = await GetTestSessionAsync())
            {
                var script = session.GetResource("OneLineOneColumn.py");
                var output = await session.ExecuteSparkCodeAsync(script);

                //await RunMirrorAsync(session, db, "delta", "OneLineTable");
            }
        }
    }
}