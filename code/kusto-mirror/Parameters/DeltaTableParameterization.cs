namespace Kusto.Mirror.ConsoleApp.Parameters
{
    public class DeltaTableParameterization
    {
        public DeltaTableParameterization(
            Uri deltaTableUrl,
            string database,
            string kustoTable,
            bool ingestPartitionColumns)
        {
            DeltaTableUrl = deltaTableUrl;
            Database = database;
            KustoTable = kustoTable;
            IngestPartitionColumns = ingestPartitionColumns;
        }

        public Uri DeltaTableUrl { get; }

        public string Database { get; }

        public string KustoTable { get; }

        public bool IngestPartitionColumns { get; }
    }
}