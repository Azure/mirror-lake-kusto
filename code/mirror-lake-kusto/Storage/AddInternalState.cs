using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MirrorLakeKusto.Storage
{
    internal class AddInternalState
    {
        /// <summary>Extents where the data of the blob has been ingested in staging.</summary>
        public IImmutableList<Guid>? StagingExtentIds { get; set; }
    }
}