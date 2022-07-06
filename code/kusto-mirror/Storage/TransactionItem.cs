using System.Collections.Immutable;

namespace Kusto.Mirror.ConsoleApp.Storage
{
    internal class TransactionItem
    {
        #region Constructors
        private TransactionItem(
            string kustoTableName,
            int startTxId,
            int endTxId,
            TransactionItemAction action,
            TransactionItemState state,
            DateTime timestamp,
            DateTime? ingestionTime,
            string? blobPath,
            IImmutableDictionary<string, string>? partitionValues,
            long? size,
            long? recordCount,
            Guid? deltaTableId,
            string? deltaTableName,
            IImmutableList<string>? partitionColumns,
            IImmutableDictionary<string, string>? schema)
        {
            KustoTableName = kustoTableName;
            StartTxId = startTxId;
            EndTxId = endTxId;
            Action = action;
            State = state;
            Timestamp = timestamp;
            IngestionTime = ingestionTime;
            BlobPath = blobPath;
            PartitionValues = partitionValues;
            Size = size;
            RecordCount = recordCount;
            DeltaTableId = deltaTableId;
            DeltaTableName = deltaTableName;
            PartitionColumns = partitionColumns;
            Schema = schema;
        }

        public static TransactionItem CreateAddItem(
            string kustoTableName,
            int startTxId,
            int endTxId,
            TransactionItemState state,
            DateTime timestamp,
            DateTime? ingestionTime,
            string blobPath,
            IImmutableDictionary<string, string> partitionValues,
            long size,
            long recordCount)
        {
            return new TransactionItem(
                kustoTableName,
                startTxId,
                endTxId,
                TransactionItemAction.Add,
                state,
                timestamp,
                ingestionTime,
                blobPath,
                partitionValues,
                size,
                recordCount,
                null,
                null,
                null,
                null);
        }

        public static TransactionItem CreateRemoveItem(
            string kustoTableName,
            int startTxId,
            int endTxId,
            TransactionItemState state,
            DateTime timestamp,
            DateTime? ingestionTime,
            string blobPath,
            IImmutableDictionary<string, string> partitionValues,
            long size)
        {
            return new TransactionItem(
                kustoTableName,
                startTxId,
                endTxId,
                TransactionItemAction.Remove,
                state,
                timestamp,
                ingestionTime,
                blobPath,
                partitionValues,
                size,
                null,
                null,
                null,
                null,
                null);
        }

        public static TransactionItem CreateSchemaItem(
            string kustoTableName,
            int startTxId,
            int endTxId,
            TransactionItemState state,
            DateTime timestamp,
            DateTime? ingestionTime,
            Guid deltaTableId,
            string deltaTableName,
            IImmutableList<string> partitionColumns,
            IImmutableDictionary<string, string> schema)
        {
            return new TransactionItem(
                kustoTableName,
                startTxId,
                endTxId,
                TransactionItemAction.Schema,
                state,
                timestamp,
                ingestionTime,
                null,
                null,
                null,
                null,
                deltaTableId,
                deltaTableName,
                partitionColumns,
                schema);
        }
        #endregion

        #region Common properties
        /// <summary>Name of the table in Kusto.</summary>
        public string KustoTableName { get; }

        /// <summary>Start of the transaction range.</summary>
        public int StartTxId { get; }

        /// <summary>End of the transaction range.</summary>
        public int EndTxId { get; }

        /// <summary>Action to be done.</summary>
        public TransactionItemAction Action { get; }

        /// <summary>State of the action (depends on <see cref="Action"/>).</summary>
        public TransactionItemState State { get; }

        /// <summary>
        /// For schema:  creation time of the table.
        /// For add:  modification time.
        /// For remove:  deletion time.
        /// </summary>
        public DateTime Timestamp { get; }

        /// <summary>Time of ingestion when read from Kusto, otherwise <c>null</c>.</summary>
        public DateTime? IngestionTime { get; }
        #endregion

        #region Add / Remove common properties
        /// <summary>Path to the blob to add / remove.</summary>
        public string? BlobPath { get; }

        /// <summary>Partition values for the data being added / removed.</summary>
        public IImmutableDictionary<string, string>? PartitionValues { get; }
        
        /// <summary>Size in byte of the blob to add / remove.</summary>
        public long? Size { get; }
        #endregion

        #region Add only
        /// <summary>Number of records in the blob to add.</summary>
        public long? RecordCount { get; }
        #endregion

        #region Schema only
        /// <summary>Unique id of the delta table (in Spark).</summary>
        public Guid? DeltaTableId { get; }

        /// <summary>Unique id of the delta table (in Spark).</summary>
        public string? DeltaTableName { get; }

        /// <summary>List of the partition columns.</summary>
        public IImmutableList<string>? PartitionColumns { get; }

        /// <summary>Schema of the table:  types for each column.</summary>
        public IImmutableDictionary<string, string>? Schema { get; set; }
        #endregion
    }
}