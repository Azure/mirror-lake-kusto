namespace Kusto.Mirror.ConsoleApp.Storage.DeltaTable
{
    internal class TransactionLog
    {
        public static TransactionLog LoadLog(int txId, string blobText)
        {
            var lines = blobText.Split('\n');

            throw new NotImplementedException();
        }

        public TransactionLog()
        {
        }

        public int StartTxId { get; }
        
        public int EndTxId { get; }
    }
}