using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MirrorLakeKustoTest.Electric
{
    public class ElectricTest : TestBase
    {
        [Fact]
        public async Task StraightLoad()
        {
            await using (var session = await GetTestSessionAsync("delta", "Electric"))
            {
                var script = session.GetResource("StraightLoad.py");
                var output = await session.ExecuteSparkCodeAsync(script);

                await session.RunMirrorAsync();

                var rowCounts = await session.ExecuteQueryAsync(
                    "| count",
                    r => (long)r[0]);

                Assert.Equal(467855, rowCounts.First());
            }
        }

        [Fact]
        public async Task PartitionLoad()
        {
            await using (var session = await GetTestSessionAsync("delta", "Electric"))
            {
                var script = session.GetResource("PartitionLoad.py");
                var output = await session.ExecuteSparkCodeAsync(script);

                await session.RunMirrorAsync();

                var rowCounts = await session.ExecuteQueryAsync(
                    "| where ModelYear==1994 | count",
                    r => (long)r[0]);

                Assert.Equal(3, rowCounts.First());
            }
        }
    }
}