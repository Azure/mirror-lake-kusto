using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kusto.Mirror.ConsoleApp.Storage
{
    /// <summary>
    /// Based on https://github.com/delta-io/delta/blob/master/PROTOCOL.md#actions
    /// </summary>
    internal class TransactionLogEntry
    {
        #region Inner Types
        public class MetadataData
        {
            #region Inner Types
            public class FormatData
            {
                public string? Provider { get; set; }

                public IDictionary<string, string>? Options { get; set; }
            }

            public class StructTypeData
            {
                public string Type { get; set; } = string.Empty;

                public StructFieldData[]? Fields { get; set; }
            }

            public class StructFieldData
            {
                public string Name { get; set; } = string.Empty;

                public string Type { get; set; } = string.Empty;
            }
            #endregion

            public Guid Id { get; set; }

            public string Name { get; set; } = string.Empty;

            public FormatData? Format { get; set; }

            public string SchemaString { get; set; } = string.Empty;

            public string[]? PartitionColumns { get; set; }

            public long CreatedTime { get; set; }

            public IDictionary<string, string>? Configuration { get; set; }
        }

        public class AddData
        {
            #region Inner Types
            public class StatsData
            {
                public long NumRecords { get; set; }
            }
            #endregion

            public string Path { get; set; } = string.Empty;

            public IDictionary<string, string>? PartitionValues { get; set; }

            public long Size { get; set; }

            public long ModificationTime { get; set; }

            public bool DataChange { get; set; }

            public string Stats { get; set; } = string.Empty;

            public IDictionary<string, string>? Tags { get; set; }
        }

        public class RemoveData
        {
            public string Path { get; set; } = string.Empty;

            public long DeletionTimestamp { get; set; }

            public bool DataChange { get; set; }

            public bool ExtendedFileMetadata { get; set; }

            public IDictionary<string, string>? PartitionValues { get; set; }

            public long Size { get; set; }

            public IDictionary<string, string>? Tags { get; set; }
        }

        public class TxnData
        {
            public string AppId { get; set; } = string.Empty;

            public long Version { get; set; }

            public long LastUpdated { get; set; }
        }

        public class ProtocolData
        {
            public int MinReaderVersion { get; set; }

            public int MinWriterVersion { get; set; }
        }

        public class CommitInfoData
        {
        }
        #endregion

        public MetadataData? Metadata { get; set; }

        public AddData? Add { get; set; }

        public RemoveData? Remove { get; set; }

        public TxnData? Txn { get; set; }

        public ProtocolData? Protocol { get; set; }

        public CommitInfoData? CommitInfo { get; set; }
    }
}