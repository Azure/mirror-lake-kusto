using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.Configuration.Attributes;
using CsvHelper.TypeConversion;
using System.Collections.Immutable;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace MirrorLakeKusto.Storage
{
    /// <summary>Leverages https://joshclose.github.io/CsvHelper/.</summary>
    internal class TransactionItem
    {
        private readonly static TransactionItemSerializerContext _transactionItemSerializerContext =
            new TransactionItemSerializerContext(
                new JsonSerializerOptions
                {
                    WriteIndented = true
                });

        public static string ExternalTableSchema => "KustoDatabaseName:string, KustoTableName:string, StartTxId:long, EndTxId:long, Action:string, State:string, MirrorTimestamp:datetime, DeltaTimestamp:datetime, BlobPath:string, PartitionValues:dynamic, Size:long, RecordCount:long, DeltaTableId:guid, DeltaTableName:string, PartitionColumns:string, Schema:string, StagingTableName:string";

        #region Inner types
        private class DictionaryConverter : DefaultTypeConverter
        {
            public override object? ConvertFromString(
                string text,
                IReaderRow row,
                MemberMapData memberMapData)
            {
                if (string.IsNullOrWhiteSpace(text))
                {
                    return ImmutableDictionary<string, string>.Empty;
                }
                else
                {
                    var map = JsonSerializer.Deserialize(
                        text,
                        typeof(IImmutableDictionary<string, string>),
                        _transactionItemSerializerContext);

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

                if (map != null)
                {
                    var text = JsonSerializer.Serialize(
                        map,
                        typeof(IImmutableDictionary<string, string>),
                        _transactionItemSerializerContext);

                    return text;
                }
                else
                {
                    return string.Empty;
                }
            }
        }

        private class ListConverter<T> : DefaultTypeConverter
        {
            public override object? ConvertFromString(
                string text,
                IReaderRow row,
                MemberMapData memberMapData)
            {
                if (string.IsNullOrWhiteSpace(text))
                {
                    return ImmutableArray<T>.Empty;
                }
                else
                {
                    var array = JsonSerializer.Deserialize(
                        text,
                        typeof(IImmutableList<T>),
                        _transactionItemSerializerContext);

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

                if (list != null)
                {
                    var text = JsonSerializer.Serialize(
                        list,
                        typeof(IImmutableList<T>),
                        _transactionItemSerializerContext);

                    return text;
                }
                else
                {
                    return string.Empty;
                }
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
            MirrorTimestamp = DateTime.MinValue;
        }

        private TransactionItem(
            string kustoDatabaseName,
            string kustoTableName,
            long startTxId,
            long endTxId,
            TransactionItemAction action,
            TransactionItemState state,
            DateTime? deltaTimestamp,
            Uri? blobPath,
            IImmutableDictionary<string, string>? partitionValues,
            long? size,
            long? recordCount,
            IImmutableList<string>? partitionColumns,
            IImmutableList<ColumnDefinition>? schema,
            InternalState internalState)
        {
            KustoDatabaseName = kustoDatabaseName;
            KustoTableName = kustoTableName;
            StartTxId = startTxId;
            EndTxId = endTxId;
            Action = action;
            State = state;
            DeltaTimestamp = deltaTimestamp;
            MirrorTimestamp = DateTime.UtcNow;
            BlobPath = blobPath;
            PartitionValues = partitionValues;
            Size = size;
            RecordCount = recordCount;
            PartitionColumns = partitionColumns;
            Schema = schema;
            InternalState = internalState;
        }

        public static TransactionItem CreateStagingTableItem(
            string kustoDatabaseName,
            string kustoTableName,
            long startTxId,
            long endTxId,
            TransactionItemState state,
            StagingTableInternalState stagingTableInternalState)
        {
            return new TransactionItem(
                kustoDatabaseName,
                kustoTableName,
                startTxId,
                endTxId,
                TransactionItemAction.StagingTable,
                state,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                new InternalState { StagingTableInternalState = stagingTableInternalState });
        }

        public static TransactionItem CreateAddItem(
            string kustoDatabaseName,
            string kustoTableName,
            long startTxId,
            long endTxId,
            TransactionItemState state,
            DateTime deltaTimestamp,
            Uri blobPath,
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
                deltaTimestamp,
                blobPath,
                partitionValues,
                size,
                recordCount,
                null,
                null,
                new InternalState { AddInternalState = new AddInternalState() });
        }

        public static TransactionItem CreateRemoveItem(
            string kustoDatabaseName,
            string kustoTableName,
            long startTxId,
            long endTxId,
            TransactionItemState state,
            DateTime deltaTimestamp,
            Uri blobPath,
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
                deltaTimestamp,
                blobPath,
                partitionValues,
                size,
                null,
                null,
                null,
                new InternalState { });
        }

        public static TransactionItem CreateSchemaItem(
            string kustoDatabaseName,
            string kustoTableName,
            long startTxId,
            long endTxId,
            TransactionItemState state,
            DateTime deltaTimestamp,
            IImmutableList<string> partitionColumns,
            IImmutableList<ColumnDefinition>? schema,
            SchemaInternalState schemaInternalState)
        {
            return new TransactionItem(
                kustoDatabaseName,
                kustoTableName,
                startTxId,
                endTxId,
                TransactionItemAction.Schema,
                state,
                deltaTimestamp,
                null,
                null,
                null,
                null,
                partitionColumns,
                schema,
                new InternalState { SchemaInternalState = schemaInternalState });
        }
        #endregion

        #region Common properties
        /// <summary>Name of the database in Kusto.</summary>
        [Index(100)]
        public string KustoDatabaseName { get; set; }

        /// <summary>Name of the table in Kusto.</summary>
        [Index(200)]
        public string KustoTableName { get; set; }

        /// <summary>Start of the transaction range.</summary>
        [Index(300)]
        public long StartTxId { get; set; }

        /// <summary>End of the transaction range.</summary>
        [Index(400)]
        public long EndTxId { get; set; }

        /// <summary>Action to be done.</summary>
        [Index(500)]
        public TransactionItemAction Action { get; set; }

        /// <summary>State of the action (depends on <see cref="Action"/>).</summary>
        [Index(600)]
        public TransactionItemState State { get; set; }

        /// <summary>Time this item was created.</summary>
        [Index(700)]
        public DateTime MirrorTimestamp { get; set; }
        #endregion

        #region DeltaTimestamp
        /// <summary>Time recorded in the Delta Table.</summary>
        /// <summary>
        /// For schema:  creation time of the table.
        /// For staging table:  doesn't exist.
        /// For add:  modification time.
        /// For remove:  deletion time.
        /// </summary>
        [Index(800)]
        public DateTime? DeltaTimestamp { get; set; }
        #endregion

        #region Add / Remove common properties
        /// <summary>Path to the blob to add / remove.</summary>
        [Index(1000)]
        public Uri? BlobPath { get; set; }

        /// <summary>Partition values for the data being added / removed.</summary>
        [TypeConverter(typeof(DictionaryConverter))]
        [Index(1100)]
        public IImmutableDictionary<string, string>? PartitionValues { get; set; }

        /// <summary>Size in byte of the blob to add / remove.</summary>
        [Index(1200)]
        public long? Size { get; set; }
        #endregion

        #region Add only
        /// <summary>Number of records in the blob to add.</summary>
        [Index(1300)]
        public long? RecordCount { get; set; }
        #endregion

        #region Schema only
        /// <summary>List of the partition columns.</summary>
        [Index(1700)]
        [TypeConverter(typeof(ListConverter<string>))]
        public IImmutableList<string>? PartitionColumns { get; set; }

        /// <summary>Schema of the table:  types for each column.</summary>
        [Index(1800)]
        [TypeConverter(typeof(ListConverter<ColumnDefinition>))]
        public IImmutableList<ColumnDefinition>? Schema { get; set; }
        #endregion

        #region InternalState
        /// <summary>Internal state ; implementation details.</summary>
        /// <remarks>This was put in place to reduce the number of columns.</remarks>
        [Index(10000)]
        public InternalState InternalState { get; set; } = new InternalState();
        #endregion

        public TransactionItem UpdateState(TransactionItemState applied)
        {
            var clone = Clone(clone =>
            {
                clone.State = applied;
                clone.MirrorTimestamp = DateTime.UtcNow;
            });

            return clone;
        }

        public TransactionItem Clone(Action<TransactionItem>? action = null)
        {
            var clone = (TransactionItem)MemberwiseClone();

            if (action != null)
            {
                action(clone);
            }

            return clone;
        }

        public static ReadOnlyMemory<byte> GetCsvHeader()
        {
            using (var stream = new MemoryStream())
            using (var writer = new StreamWriter(stream))
            using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
            {
                var builder = new StringBuilder();
                var csv2 = new CsvWriter(new StringWriter(builder), CultureInfo.InvariantCulture);

                csv2.WriteHeader<TransactionItem>();
                csv2.NextRecord();
                csv2.Flush();

                var text = builder.ToString();

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
    }

    [JsonSerializable(typeof(IImmutableList<ColumnDefinition>))]
    [JsonSerializable(typeof(IImmutableList<string>))]
    [JsonSerializable(typeof(IImmutableList<Guid>))]
    [JsonSerializable(typeof(IImmutableDictionary<string, string>))]
    [JsonSerializable(typeof(InternalState))]
    internal partial class TransactionItemSerializerContext : JsonSerializerContext
    {
    }
}