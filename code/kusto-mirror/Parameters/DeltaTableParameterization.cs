namespace Kusto.Mirror.ConsoleApp.Parameters
{
    public class DeltaTableParameterization
    {
        public DeltaTableParameterization(
            Uri checkpointBlobUrl,
            Uri deltaTableStorageUrl,
            string database,
            string kustoTable,
            bool ingestPartitionColumns)
        {
            CheckpointBlobUrl = checkpointBlobUrl;
            DeltaTableStorageUrl = deltaTableStorageUrl;
            Database = database;
            KustoTable = kustoTable;
            IngestPartitionColumns = ingestPartitionColumns;
        }

        public Uri CheckpointBlobUrl { get; }
        
        public Uri DeltaTableStorageUrl { get; }

        public string Database { get; }

        public string KustoTable { get; }

        public bool IngestPartitionColumns { get; }
    }
}