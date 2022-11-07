using Microsoft.Azure.Management.Kusto.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MirrorLakeKustoTest.Electric
{
    public class DeleteTest : ElectricTestBase
    {
        [Fact]
        public async Task DeleteWithPartitionOneGo()
        {
            await using (var session = await GetTestSessionAsync("delta", "Electric"))
            {
                var script1 = session.GetResource("PartitionLoad.py");
                var script2 = session.GetResource("DeleteWithPartition.py");
                var output1 = await session.ExecuteSparkCodeAsync(script1);
                var output2 = await session.ExecuteSparkCodeAsync(script2);

                await session.RunMirrorAsync();

                var rowCounts = await session.ExecuteQueryAsync(
                    "| count",
                    r => (long)r[0]);

                Assert.Equal(467145, rowCounts.First());
            }
        }

        [Fact]
        public async Task DeleteWithPartitionTwoShots()
        {
            await using (var session = await GetTestSessionAsync("delta", "Electric"))
            {
                var script1 = session.GetResource("PartitionLoad.py");
                var script2 = session.GetResource("DeleteWithPartition.py");
                var output1 = await session.ExecuteSparkCodeAsync(script1);

                await session.RunMirrorAsync();

                var output2 = await session.ExecuteSparkCodeAsync(script2);

                await session.RunMirrorAsync();

                var rowCounts = await session.ExecuteQueryAsync(
                    "| count",
                    r => (long)r[0]);

                Assert.Equal(467145, rowCounts.First());
            }
        }

        [Fact]
        public async Task DeleteTwoShotsWithSkipped()
        {
            await using (var session = await GetTestSessionAsync("delta", "Electric"))
            {
                var script1 = session.GetResource("PartitionLoad.py");
                var script2 = session.GetResource("DeleteYear2020.py");
                var output1 = await session.ExecuteSparkCodeAsync(script1);

                await session.RunMirrorAsync(CreationTimeExpression, "01-01-2020");

                var rowCounts1 = await session.ExecuteQueryAsync(
                    "| count",
                    r => (long)r[0]);
                var output2 = await session.ExecuteSparkCodeAsync(script2);

                await session.RunMirrorAsync(CreationTimeExpression, "01-01-2020");

                var rowCounts2 = await session.ExecuteQueryAsync(
                    "| count",
                    r => (long)r[0]);

                Assert.Equal(32004 + 29068 + 791, rowCounts1.First());
                Assert.Equal(29068 + 791, rowCounts2.First());
            }
        }

        [Fact]
        public async Task DeleteSkippedTwoShots()
        {
            await using (var session = await GetTestSessionAsync("delta", "Electric"))
            {
                var script1 = session.GetResource("PartitionLoad.py");
                var script2 = session.GetResource("DeleteYear2020.py");
                var output1 = await session.ExecuteSparkCodeAsync(script1);

                await session.RunMirrorAsync(CreationTimeExpression, "01-01-2021");

                var rowCounts1 = await session.ExecuteQueryAsync(
                    "| count",
                    r => (long)r[0]);
                var output2 = await session.ExecuteSparkCodeAsync(script2);

                await session.RunMirrorAsync(CreationTimeExpression, "01-01-2021");

                var rowCounts2 = await session.ExecuteQueryAsync(
                    "| count",
                    r => (long)r[0]);

                //  We skipped 2020, it shouldn't have impact
                Assert.Equal(29068 + 791, rowCounts1.First());
                Assert.Equal(29068 + 791, rowCounts2.First());
            }
        }
    }
}