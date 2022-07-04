namespace Kusto.Mirror.ConsoleApp.Storage.DeltaTable
{
    internal class TransactionRemove
    {
        private TransactionLogEntry.RemoveData? remove;

        public TransactionRemove(TransactionLogEntry.RemoveData remove)
        {
            this.remove = remove;
        }
    }
}