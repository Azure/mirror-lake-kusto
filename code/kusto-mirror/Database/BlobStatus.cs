namespace Kusto.Mirror.ConsoleApp.Database
{
    internal class BlobStatus
    {
        public BlobStatus(
            string tableName,
            int txId,
            string blobPath,
            string action,
            string status,
            DateTime ingestionTime)
        {
            TableName = tableName;
            TxId = txId;
            BlobPath = blobPath;
            Action = action;
            Status = status;
            IngestionTime = ingestionTime;
        }

        public string TableName { get; }

        public int TxId { get; }

        public string BlobPath { get; }

        public string Action { get; }

        public string Status { get; }
 
        public DateTime IngestionTime { get; }
    }
}