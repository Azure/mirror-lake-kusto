using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MirrorLakeKusto.Storage
{
    internal class StagingTableInternalState
    {
        /// <summary>Name of the Kusto staging table.</summary>
        public string? StagingTableName { get; set; }
    }
}
