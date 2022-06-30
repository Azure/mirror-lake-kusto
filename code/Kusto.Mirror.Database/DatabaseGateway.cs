namespace Kusto.Mirror.Database
{
    public class DatabaseGateway
    {
        private readonly KustoClusterGateway _clusterGateway;
        private readonly string _database;

        public DatabaseGateway(KustoClusterGateway clusterGateway, string database)
        {
            _clusterGateway = clusterGateway;
            _database = database;
        }

        public async Task CreateMergeDatabaseObjectsAsync(CancellationToken ct)
        {
            await _clusterGateway.ExecuteCommandAsync(
                _database,
                ".create-merge table KM_DeltaTables(TxId:int)",
                ct);
        }
    }
}