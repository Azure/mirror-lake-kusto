using CsvHelper.Configuration.Attributes;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MirrorLakeKusto.Storage
{
    internal class InternalState
    {
        public StagingTableInternalState? StagingTable { get; set; }

        public AddInternalState? Add { get; set; }

        public SchemaInternalState? Schema { get; set; }
    }
}