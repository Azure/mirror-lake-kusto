namespace KustoMirrorTest.Simple
{
    public class Simple : TestBase
    {
        [Fact]
        public async Task OneLineOneColumn()
        {
            await using (var session = await GetSparkSessionAsync())
            {
                var dbTask = session.CreateDbAsync();
                var script = session.GetResource("OneLineOneColumn.py");
                var output = await session.ExecuteSparkCodeAsync(script);
                var db = await dbTask;
            }
        }
    }
}