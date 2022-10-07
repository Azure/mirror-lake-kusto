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
        public StagingTableInternalState? StagingTableInternalState { get; set; }

        public AddInternalState? AddInternalState { get; set; }

        public SchemaInternalState? SchemaInternalState { get; set; }
    }
}