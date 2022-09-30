using Azure.Analytics.Synapse.Spark.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MirrorLakeKustoTest
{
    internal class SparkSingleton
    {
        #region Constructors
        static SparkSingleton()
        {
            Instance = new SparkSingleton();
        }

        private SparkSingleton()
        {
        }
        #endregion

        public static SparkSingleton Instance { get; }

        public async Task<SparkSession> GetSparkSessionAsync()
        {
            //var sparkPoolName = "quicktests";
            await Task.CompletedTask;

            throw new NotImplementedException();
        }
    }
}