using Kusto.Mirror.ConsoleApp.Database;
using Kusto.Mirror.ConsoleApp.Storage.DeltaTable;

namespace Kusto.Mirror.ConsoleApp
{
    internal class DeltaTableOrchestration
    {
        private readonly TableStatus _tableStatus;
        private readonly DeltaTableGateway _deltaTableGateway;

        public DeltaTableOrchestration(
            TableStatus tableStatus,
            DeltaTableGateway deltaTableGateway)
        {
            _tableStatus = tableStatus;
            _deltaTableGateway = deltaTableGateway;
        }

        public string KustoTableName => _tableStatus.TableName;

        internal async Task RunAsync(CancellationToken ct)
        {
            await Task.CompletedTask;
        }
    }
}