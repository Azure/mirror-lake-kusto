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
                    Assert.Contains((long)i, ids);
                }
            }
        }

        [Fact]
        public async Task CheckpointTxOneShot()
        {
            await using (var session = await GetTestSessionAsync("delta", "Checkpointed"))
            {
                var script1 = session.GetResource("SetupCheckpointTx.py");
                var output1 = await session.ExecuteSparkCodeAsync(script1);
                var script2 = session.GetResource("DoingCheckpointTx.py");
                var output2 = await session.ExecuteSparkCodeAsync(script1);

                await session.RunMirrorAsync();

                var ids = await session.ExecuteQueryAsync(
                    "",
                    r => (long)r["id"]);

                Assert.Equal(11, ids.Count);
                for (int i = 0; i != 11; ++i)
                {
                    Assert.Contains((long)i, ids);
                }
            }
        }

        [Fact]
        public async Task CheckpointTxWithDelta()
        {
            await using (var session = await GetTestSessionAsync("delta", "Checkpointed"))
            {
                var script1 = session.GetResource("SetupCheckpointTx.py");
                var output1 = await session.ExecuteSparkCodeAsync(script1);
                
                await session.RunMirrorAsync();
                
                var script2 = session.GetResource("DoingCheckpointTx.py");
                var output2 = await session.ExecuteSparkCodeAsync(script1);

                await session.RunMirrorAsync();

                var ids = await session.ExecuteQueryAsync(
                    "",
                    r => (long)r["id"]);

                Assert.Equal(11, ids.Count);
                for (int i = 0; i != 11; ++i)
                {
                    Assert.Contains((long)i, ids);
                }
            }
        }
    }
}