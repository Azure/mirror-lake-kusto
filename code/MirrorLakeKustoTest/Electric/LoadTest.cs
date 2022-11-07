using Microsoft.Azure.Management.Kusto.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MirrorLakeKustoTest.Electric
{
    public class LoadTest : ElectricTestBase
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
        public async Task StraightLoadOptimizeOneGo()
        {
            await using (var session = await GetTestSessionAsync("delta", "Electric"))
            {
                var script1 = session.GetResource("StraightLoad.py");
                var script2 = session.GetResource("Optimize.py");
                var output1 = await session.ExecuteSparkCodeAsync(script1);
                var output2 = await session.ExecuteSparkCodeAsync(script2);

                await session.RunMirrorAsync();

                var rowCounts = await session.ExecuteQueryAsync(
                    "| count",
                    r => (long)r[0]);

                Assert.Equal(467855, rowCounts.First());
            }
        }

        [Fact]
        public async Task StraightLoadOptimizeTwoShots()
        {
            await using (var session = await GetTestSessionAsync("delta", "Electric"))
            {
                var script1 = session.GetResource("StraightLoad.py");
                var script2 = session.GetResource("Optimize.py");
                var output1 = await session.ExecuteSparkCodeAsync(script1);

                await session.RunMirrorAsync();

                var output2 = await session.ExecuteSparkCodeAsync(script2);

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