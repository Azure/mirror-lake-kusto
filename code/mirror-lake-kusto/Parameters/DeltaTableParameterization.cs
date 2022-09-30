namespace Kusto.Mirror.ConsoleApp.Parameters
{
    public class DeltaTableParameterization
    {
        public DeltaTableParameterization(
            Uri deltaTableStorageUrl,
            string database,
            string kustoTable,
            bool ingestPartitionColumns)
        {
            DeltaTableStorageUrl = deltaTableStorageUrl;
            Database = database;
            KustoTable = kustoTable;
            IngestPartitionColumns = ingestPartitionColumns;
        }

        public Uri DeltaTableStorageUrl { get; }

        public string Database { get; }

        public string KustoTable { get; }

        public bool IngestPartitionColumns { get; }
    }
}