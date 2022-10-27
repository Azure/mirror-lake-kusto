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
        /// <summary>Creation time override if a function was provided.</summary>
        public DateTime? CreationTime { get; set; }

        /// <summary>Extent ID where the data of the blob has been ingested in staging.</summary>
        public Guid? StagingExtentId { get; set; }

        /// <summary>Ingestion time of the blob.  Useful to time filter at deletion.</summary>
        /// <remarks>This is a string representation from Kusto.</remarks>
        public string? IngestionTime { get; set; }
    }
}