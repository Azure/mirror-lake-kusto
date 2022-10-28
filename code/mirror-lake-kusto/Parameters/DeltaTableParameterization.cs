namespace MirrorLakeKusto.Parameters
{
    public class DeltaTableParameterization
    {
        public DeltaTableParameterization(
            Uri deltaTableStorageUrl,
            string database,
            string kustoTable,
            string? creationTime)
        {
            DeltaTableStorageUrl = deltaTableStorageUrl;
            Database = database;
            KustoTable = kustoTable;
            CreationTime = creationTime;
        }

        public Uri DeltaTableStorageUrl { get; }

        public string Database { get; }

        public string KustoTable { get; }

        public string? CreationTime { get; }
    }
}