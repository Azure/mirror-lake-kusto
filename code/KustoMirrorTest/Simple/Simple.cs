namespace KustoMirrorTest.Simple
{
    public class Simple : TestBase
    {
        [Fact]
        public async Task OneLineOneColumn()
        {
            await using (var sessionHolding = await GetSparkSessionAsync())
            {
                var script = GetResource("OneLineOneColumn.py");
                var output = await sessionHolding.ExecuteSparkCodeAsync(@"print(""hi"")");
            }
        }
    }
}