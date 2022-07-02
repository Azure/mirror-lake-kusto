namespace Kusto.Mirror.ConsoleApp.Database
{
    internal class BlobStatus
    {
        public BlobStatus(
            string tableName,
            int startTxId,
            int endTxId,
            string blobPath,
            string action,
            string status,
            DateTime ingestionTime)
        {
            TableName = tableName;
            StartTxId = startTxId;
            EndTxId = endTxId;
            BlobPath = blobPath;
            Action = action;
            Status = status;
            IngestionTime = ingestionTime;
        }

        public string TableName { get; }

        public int StartTxId { get; }
        
        public int EndTxId { get; }

        public string BlobPath { get; }

        public string Action { get; }

        public string Status { get; }
 
        public DateTime IngestionTime { get; }
    }
}