using MirrorLakeKusto.Kusto;
using MirrorLakeKusto.Storage;

namespace MirrorLakeKusto
{
    internal class BlobStagingOrchestration
    {
        private readonly DatabaseGateway _databaseGateway;
        private readonly TableDefinition _stagingTable;
        private readonly TableStatus _tableStatus;
        private readonly long _startTxId;

        #region Constructors
        public static async Task EnsureAllStagedAsync(
            DatabaseGateway databaseGateway,
            TableDefinition stagingTable,
            TableStatus tableStatus,
            long startTxId,
            CancellationToken ct)
        {
            var orchestration = new BlobStagingOrchestration(
                databaseGateway,
                stagingTable,
                tableStatus,
                startTxId);

            await orchestration.RunAsync(ct);
        }

        private BlobStagingOrchestration(
            DatabaseGateway databaseGateway,
            TableDefinition stagingTable,
            TableStatus tableStatus,
            long startTxId)
        {
            _databaseGateway = databaseGateway;
            _stagingTable = stagingTable;
            _tableStatus = tableStatus;
            _startTxId = startTxId;
        }
        #endregion

        #region Orchestration
        private async Task RunAsync(CancellationToken ct)
        {
            await Task.CompletedTask;

            throw new NotImplementedException();
        }
        #endregion
    }
}