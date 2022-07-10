using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.Configuration.Attributes;
using CsvHelper.TypeConversion;
using System.Collections.Immutable;
using System.Globalization;
using System.Text;
using System.Text.Json;

namespace Kusto.Mirror.ConsoleApp.Storage
{
    /// <summary>Leverages https://joshclose.github.io/CsvHelper/.</summary>
    internal class TransactionItem
    {
        public static string ExternalTableSchema =>
            "KustoDatabaseName:string,KustoTableName:string,StartTxId:int,EndTxId:int,"
            + "Action:string,State:string,DeltaTimestamp:datetime,MirrorTimestamp:datetime,"
            + "BlobPath:string,"
            + "PartitionValues:dynamic,Size:long,RecordCount:long,ExtentId:string,"
            + "DeltaTableId:string,"
            + "DeltaTableName:string,PartitionColumns:dynamic,Schema:dynamic";

        #region Inner types
        private class DictionaryConverter : DefaultTypeConverter
        {
            public override object? ConvertFromString(
                string text,
                IReaderRow row,
                MemberMapData memberMapData)
            {
                if (text == "null")
                {
                    return null;
                }
                else
                {
                    var map = JsonSerializer.Deserialize<IImmutableDictionary<string, string>>(text);

                    if (map == null)
                    {
                        throw new MirrorException($"Can't deserialize dictionary:  '{text}'");
                    }

                    return map;
                }
            }

            public override string? ConvertToString(
                object value,
                IWriterRow row,
                MemberMapData memberMapData)
            {
                var map = (IImmutableDictionary<string, string>)value;
                var text = JsonSerializer.Serialize<IImmutableDictionary<string, string>>(map);

                return text;
            }
        }

        private class ListConverter<T> : DefaultTypeConverter
        {
            public override object? ConvertFromString(
                string text,
                IReaderRow row,
                MemberMapData memberMapData)
            {
                if (text == "null")
                {
                    return null;
                }
                else
                {
                    var array = JsonSerializer.Deserialize<IImmutableList<T>>(text);

                    if (array == null)
                    {
                        throw new MirrorException($"Can't deserialize list:  '{text}'");
                    }

                    return array;
                }
            }

            public override string? ConvertToString(
                object value,
                IWriterRow row,
                MemberMapData memberMapData)
            {
                var list = (IImmutableList<T>)value;
                var text = JsonSerializer.Serialize<IImmutableList<T>>(list);

                return text;
            }
        }
        #endregion

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
            DeltaTimestamp = DateTime.MinValue;
            MirrorTimestamp = DateTime.MinValue;
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
            IImmutableList<ColumnDefinition>? schema)
        {
            KustoDatabaseName = kustoDatabaseName;
            KustoTableName = kustoTableName;
            StartTxId = startTxId;
            EndTxId = endTxId;
            Action = action;
            State = state;
            DeltaTimestamp = timestamp;
            MirrorTimestamp = DateTime.UtcNow;
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
            //  Synapse Spark sometimes omit those on remove
            IImmutableDictionary<string, string>? partitionValues,
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
            IImmutableList<ColumnDefinition>? schema)
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
        public DateTime DeltaTimestamp { get; set; }

        /// <summary>Time this item was created.</summary>
        [Index(6)]
        public DateTime MirrorTimestamp { get; set; }
        #endregion

        #region Add / Remove common properties
        /// <summary>Path to the blob to add / remove.</summary>
        [Index(7)]
        public string? BlobPath { get; set; }

        /// <summary>Partition values for the data being added / removed.</summary>
        [TypeConverter(typeof(DictionaryConverter))]
        [Index(8)]
        public IImmutableDictionary<string, string>? PartitionValues { get; set; }

        /// <summary>Size in byte of the blob to add / remove.</summary>
        [Index(9)]
        public long? Size { get; set; }
        #endregion

        #region Add only
        /// <summary>Number of records in the blob to add.</summary>
        [Index(10)]
        public long? RecordCount { get; set; }

        [Index(11)]
        public string? ExtentId { get; set; }
        #endregion

        #region Schema only
        /// <summary>Unique id of the delta table (in Spark).</summary>
        [Index(12)]
        public Guid? DeltaTableId { get; set; }

        /// <summary>Unique id of the delta table (in Spark).</summary>
        [Index(13)]
        public string? DeltaTableName { get; set; }

        /// <summary>List of the partition columns.</summary>
        [Index(14)]
        [TypeConverter(typeof(ListConverter<string>))]
        public IImmutableList<string>? PartitionColumns { get; set; }

        /// <summary>Schema of the table:  types for each column.</summary>
        [Index(15)]
        [TypeConverter(typeof(ListConverter<ColumnDefinition>))]
        public IImmutableList<ColumnDefinition>? Schema { get; set; }
        #endregion

        public TransactionItem UpdateState(TransactionItemState applied)
        {
            var clone = Clone();

            clone.State = applied;
            clone.MirrorTimestamp = DateTime.UtcNow;

            return clone;
        }

        public static ReadOnlyMemory<byte> GetCsvHeader()
        {
            using (var stream = new MemoryStream())
            using (var writer = new StreamWriter(stream))
            using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
            {
                csv.WriteHeader<TransactionItem>();
                csv.NextRecord();
                csv.Flush();
                writer.Flush();
                stream.Flush();

                var buffer = stream.ToArray();

                return new ReadOnlyMemory<byte>(buffer);
            }
        }

        public static ReadOnlyMemory<byte> ToCsv(IEnumerable<TransactionItem> items)
        {
            using (var stream = new MemoryStream())
            using (var writer = new StreamWriter(stream))
            using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
            {
                foreach (var item in items)
                {
                    csv.WriteRecord(item);
                    csv.NextRecord();
                }
                csv.Flush();
                writer.Flush();
                stream.Flush();

                var buffer = stream.ToArray();

                return new ReadOnlyMemory<byte>(buffer);
            }
        }

        public static IImmutableList<TransactionItem> FromCsv(
            ReadOnlyMemory<byte> buffer,
            bool validateHeader)
        {
            using (var stream = new MemoryStream(buffer.ToArray()))
            using (var reader = new StreamReader(stream))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                if (validateHeader)
                {
                    csv.Read();
                    csv.ReadHeader();
                    csv.ValidateHeader<TransactionItem>();
                }
                var items = csv.GetRecords<TransactionItem>();

                return items.ToImmutableArray();
            }
        }

        private TransactionItem Clone()
        {
            return (TransactionItem)MemberwiseClone();
        }
    }
}