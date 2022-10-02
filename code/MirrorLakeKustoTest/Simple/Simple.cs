namespace MirrorLakeKustoTest.Simple
{
    public class Simple : TestBase
    {
        [Fact]
        public async Task OneLineOneColumn()
        {
            await using (var session = await GetTestSessionAsync("delta", "OneLine"))
            {
                var script = session.GetResource("OneLineOneColumn.py");
                var output = await session.ExecuteSparkCodeAsync(script);

                await session.RunMirrorAsync();

                var ids = await session.ExecuteQueryAsync(
                    "",
                    r => (long)r["id"]);

                Assert.Equal(1, ids.Count);
                Assert.Equal(0, ids.FirstOrDefault());
            }
        }

        [Fact]
        public async Task MultiLineOneColumn()
        {
            await using (var session = await GetTestSessionAsync("delta", "MultiLine"))
            {
                var script = session.GetResource("MultiLineOneColumn.py");
                var output = await session.ExecuteSparkCodeAsync(script);

                await session.RunMirrorAsync();

                var ids = await session.ExecuteQueryAsync(
                    "",
                    r => (long)r["id"]);

                Assert.Equal(10, ids.Count);
                for (int i = 0; i != 10; ++i)
                {
                    Assert.Contains(ids, i);
                }
            }
        }
    }
}