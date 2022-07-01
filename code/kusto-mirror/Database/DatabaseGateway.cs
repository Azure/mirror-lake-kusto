namespace Kusto.Mirror.ConsoleApp.Database
{
    public class DatabaseGateway
    {
        private readonly KustoClusterGateway _clusterGateway;

        public DatabaseGateway(KustoClusterGateway clusterGateway, string database)
        {
            _clusterGateway = clusterGateway;
            DatabaseName = database;
        }

        public string DatabaseName { get; }

        public async Task CreateMergeDatabaseObjectsAsync(CancellationToken ct)
        {
            await _clusterGateway.ExecuteCommandAsync(
                DatabaseName,
                ".create-merge table KM_DeltaTables(TableName:string, TxId:int, "
                + "BlobPath:string, Action:string, Status:string)",
                ct);
        }
    }
}