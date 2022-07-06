using CsvHelper;
using CsvHelper.Configuration.Attributes;
using System.Collections.Immutable;
using System.Globalization;
using System.Text;

namespace Kusto.Mirror.ConsoleApp.Storage
{
    internal class TransactionItem
    {
        #region Constructors
        /// <summary>This should only be called by serializer.</summary>
        public TransactionItem()
        {
            KustoDatabaseName = "MISSING DB";
            KustoTableName = "MISSING TABLE";
            StartTxId = -1;
            EndTxId = -1;
            Action = (TransactionItemAction)1000000;
            State = (TransactionItemState)1000000;
            Timestamp = DateTime.MinValue;
        }

        private TransactionItem(
            string kustoDatabaseName,
            string kustoTableName,
            int startTxId,
            int endTxId,
            TransactionItemAction action,
            TransactionItemState state,
            DateTime timestamp,
            string? blobPath,
            IImmutableDictionary<string, string>? partitionValues,
            long? size,
            long? recordCount,
            Guid? deltaTableId,
            string? deltaTableName,
            IImmutableList<string>? partitionColumns,
            IImmutableDictionary<string, string>? schema)
        {
            KustoDatabaseName = kustoDatabaseName;
            KustoTableName = kustoTableName;
            StartTxId = startTxId;
            EndTxId = endTxId;
            Action = action;
            State = state;
            Timestamp = timestamp;
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
            string kustoDatabaseName,
            string kustoTableName,
            int startTxId,
            int endTxId,
            TransactionItemState state,
            DateTime timestamp,
            string blobPath,
            IImmutableDictionary<string, string> partitionValues,
            long size,
            long recordCount)
        {
            return new TransactionItem(
                kustoDatabaseName,
                kustoTableName,
                startTxId,
                endTxId,
                TransactionItemAction.Add,
                state,
                timestamp,
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
            string kustoDatabaseName,
            string kustoTableName,
            int startTxId,
            int endTxId,
            TransactionItemState state,
            DateTime timestamp,
            string blobPath,
            IImmutableDictionary<string, string> partitionValues,
            long size)
        {
            return new TransactionItem(
                kustoDatabaseName,
                kustoTableName,
                startTxId,
                endTxId,
                TransactionItemAction.Remove,
                state,
                timestamp,
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
            string kustoDatabaseName,
            string kustoTableName,
            int startTxId,
            int endTxId,
            TransactionItemState state,
            DateTime timestamp,
            Guid deltaTableId,
            string deltaTableName,
            IImmutableList<string> partitionColumns,
            IImmutableDictionary<string, string> schema)
        {
            return new TransactionItem(
                kustoDatabaseName,
                kustoTableName,
                startTxId,
                endTxId,
                TransactionItemAction.Schema,
                state,
                timestamp,
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
        /// <summary>Name of the database in Kusto.</summary>
        [Index(0)]
        public string KustoDatabaseName { get; set; }

        /// <summary>Name of the table in Kusto.</summary>
        [Index(1)]
        public string KustoTableName { get; set; }

        /// <summary>Start of the transaction range.</summary>
        [Index(2)]
        public int StartTxId { get; set; }

        /// <summary>End of the transaction range.</summary>
        [Index(3)]
        public int EndTxId { get; set; }

        /// <summary>Action to be done.</summary>
        [Index(4)]
        public TransactionItemAction Action { get; set; }

        /// <summary>State of the action (depends on <see cref="Action"/>).</summary>
        [Index(5)]
        public TransactionItemState State { get; set; }

        /// <summary>
        /// For schema:  creation time of the table.
        /// For add:  modification time.
        /// For remove:  deletion time.
        /// </summary>
        [Index(6)]
        public DateTime Timestamp { get; set; }
        #endregion

        #region Add / Remove common properties
        /// <summary>Path to the blob to add / remove.</summary>
        [Index(7)]
        public string? BlobPath { get; }

        /// <summary>Partition values for the data being added / removed.</summary>
        [Index(8)]
        public IImmutableDictionary<string, string>? PartitionValues { get; }

        /// <summary>Size in byte of the blob to add / remove.</summary>
        [Index(9)]
        public long? Size { get; }
        #endregion

        #region Add only
        /// <summary>Number of records in the blob to add.</summary>
        [Index(10)]
        public long? RecordCount { get; }
        #endregion

        #region Schema only
        /// <summary>Unique id of the delta table (in Spark).</summary>
        [Index(11)]
        public Guid? DeltaTableId { get; }

        /// <summary>Unique id of the delta table (in Spark).</summary>
        [Index(12)]
        public string? DeltaTableName { get; }

        /// <summary>List of the partition columns.</summary>
        [Index(13)]
        public IImmutableList<string>? PartitionColumns { get; }

        /// <summary>Schema of the table:  types for each column.</summary>
        [Index(14)]
        public IImmutableDictionary<string, string>? Schema { get; set; }
        #endregion

        public static string GetHeader()
        {
            using (var stream = new MemoryStream())
            using (var writer = new StreamWriter(stream))
            using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
            {
                csv.WriteHeader<TransactionItem>();
                csv.Flush();
                writer.Flush();
                stream.Flush();

                var buffer = stream.ToArray();
                var text = ASCIIEncoding.UTF8.GetString(buffer);

                return text;
            }
        }
    }
}