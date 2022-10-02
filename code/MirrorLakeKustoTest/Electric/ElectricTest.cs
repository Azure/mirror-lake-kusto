using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MirrorLakeKustoTest.Electric
{
    public class ElectricTest : TestBase
    {
        private const string DATA_FILE_NAME =
            "Electric_Vehicle_Title_and_Registration_Activity.csv.gz";

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
    }
}