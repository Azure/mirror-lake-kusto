using Kusto.Mirror.ConsoleApp.Database;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Kusto.Mirror.ConsoleApp.Storage.DeltaLake
{
    /// <summary>
    /// Based on https://github.com/delta-io/delta/blob/master/PROTOCOL.md#actions
    /// </summary>
    internal class TransactionLogEntry
    {
        #region Inner Types
        public class MetadataData
        {
            #region Inner Types
            public class FormatData
            {
                public string? Provider { get; set; }

                public IDictionary<string, string>? Options { get; set; }
            }

            public class StructTypeData
            {
                public string Type { get; set; } = string.Empty;

                public StructFieldData[]? Fields { get; set; }
            }

            public class StructFieldData
            {
                public string Name { get; set; } = string.Empty;

                public string Type { get; set; } = string.Empty;
            }
            #endregion

            public Guid Id { get; set; }

            public string Name { get; set; } = string.Empty;

            public FormatData? Format { get; set; }

            public string SchemaString { get; set; } = string.Empty;

            public string[]? PartitionColumns { get; set; }

            public long CreatedTime { get; set; }

            public IDictionary<string, string>? Configuration { get; set; }
        }

        public class AddData
        {
            #region Inner Types
            public class StatsData
            {
                public long NumRecords { get; set; }
            }
            #endregion

            public string Path { get; set; } = string.Empty;

            public ImmutableDictionary<string, string>? PartitionValues { get; set; }

            public long Size { get; set; }

            public long ModificationTime { get; set; }

            public bool DataChange { get; set; }

            public string Stats { get; set; } = string.Empty;

            public IDictionary<string, string>? Tags { get; set; }
        }

        public class RemoveData
        {
            public string Path { get; set; } = string.Empty;

            public long DeletionTimestamp { get; set; }

            public bool DataChange { get; set; }

            public bool ExtendedFileMetadata { get; set; }

            public ImmutableDictionary<string, string>? PartitionValues { get; set; }

            public long Size { get; set; }

            public IDictionary<string, string>? Tags { get; set; }
        }

        public class TxnData
        {
            public string AppId { get; set; } = string.Empty;

            public long Version { get; set; }

            public long LastUpdated { get; set; }
        }

        public class ProtocolData
        {
            public int MinReaderVersion { get; set; }

            public int MinWriterVersion { get; set; }
        }

        public class CommitInfoData
        {
        }
        #endregion

        #region Load log
        public static TransactionLog LoadDeltaLog(
            int txId,
            string kustoDatabaseName,
            string kustoTableName,
            string blobText)
        {
            var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
            var lines = blobText.Split('\n');
            var entries = lines
                .Where(l => !string.IsNullOrWhiteSpace(l))
                .Select(l => JsonSerializer.Deserialize<TransactionLogEntry>(l, options)!)
                .ToImmutableArray();
            var metadata = entries
                .Where(e => e.Metadata != null)
                .Select(e => e.Metadata!)
                .ToImmutableArray();
            var add = entries.Where(e => e.Add != null).Select(e => e.Add!);
            var remove = entries.Where(e => e.Remove != null).Select(e => e.Remove!);

            if (metadata.Count() > 1)
            {
                throw new MirrorException("More than one meta data node in one transaction");
            }

            var transactionMetadata = metadata.Any()
                ? LoadMetadata(metadata.First(), txId, kustoDatabaseName, kustoTableName)
                : null;
            var transactionAdds = add
                .Select(a => LoadAdd(a, txId, kustoDatabaseName, kustoTableName))
                .ToImmutableArray();
            var transactionRemoves = remove
                .Select(a => LoadRemove(a, txId, kustoDatabaseName, kustoTableName))
                .ToImmutableArray();

            return new TransactionLog(
                transactionMetadata,
                transactionAdds,
                transactionRemoves);
        }

        private static TransactionItem LoadMetadata(
            MetadataData metadata,
            int txId,
            string kustoDatabaseName,
            string kustoTableName)
        {
            if (metadata.Format == null || metadata.Format.Provider == null)
            {
                throw new ArgumentNullException(nameof(metadata.Format.Provider));
            }
            if (!metadata.Format.Provider.Equals("parquet", StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException(
                    "Only Parquet is supported",
                    nameof(metadata.Format.Provider));
            }
            var partitionColumns = metadata.PartitionColumns == null
                ? ImmutableArray<string>.Empty
                : metadata.PartitionColumns.ToImmutableArray();
            var schema = ExtractSchema(metadata.SchemaString);
            var createdTime = DateTimeOffset
                .FromUnixTimeMilliseconds(metadata.CreatedTime)
                .UtcDateTime;

            var item = TransactionItem.CreateSchemaItem(
                kustoDatabaseName,
                kustoTableName,
                txId,
                txId,
                TransactionItemState.ToBeApplied,
                createdTime,
                metadata.Id,
                metadata.Name,
                partitionColumns,
                schema);

            return item;
        }

        private static TransactionItem LoadAdd(
            AddData addEntry,
            int txId,
            string kustoDatabaseName,
            string kustoTableName)
        {
            if (string.IsNullOrWhiteSpace(addEntry.Path))
            {
                throw new ArgumentNullException(nameof(addEntry.Path));
            }
            if (addEntry.PartitionValues == null)
            {
                throw new ArgumentNullException(nameof(addEntry.PartitionValues));
            }
            var modificationTime = DateTimeOffset
                .FromUnixTimeMilliseconds(addEntry.ModificationTime)
                .UtcDateTime;
            var recordCount = ExtractRecordCount(addEntry.Stats);
            var item = TransactionItem.CreateAddItem(
                kustoDatabaseName,
                kustoTableName,
                txId,
                txId,
                TransactionItemState.ToBeAdded,
                modificationTime,
                addEntry.Path,
                addEntry.PartitionValues,
                addEntry.Size,
                recordCount);

            return item;
        }

        private static TransactionItem LoadRemove(
            RemoveData removeEntry,
            int txId,
            string kustoDatabaseName,
            string kustoTableName)
        {
            if (string.IsNullOrWhiteSpace(removeEntry.Path))
            {
                throw new ArgumentNullException(nameof(removeEntry.Path));
            }
            //  Remove this check as Synapse Spark sometimes omit partition values on delete
            //if (removeEntry.PartitionValues == null)
            //{
            //    throw new ArgumentNullException(nameof(removeEntry.PartitionValues));
            //}
            var deletionTimestamp = DateTimeOffset
                .FromUnixTimeMilliseconds(removeEntry.DeletionTimestamp)
                .UtcDateTime;
            var item = TransactionItem.CreateRemoveItem(
                kustoDatabaseName,
                kustoTableName,
                txId,
                txId,
                TransactionItemState.ToBeAdded,
                deletionTimestamp,
                removeEntry.Path,
                removeEntry.PartitionValues,
                removeEntry.Size);

            return item;
        }
        private static IImmutableList<ColumnDefinition> ExtractSchema(string schemaString)
        {
            var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
            var typeData =
                JsonSerializer.Deserialize<TransactionLogEntry.MetadataData.StructTypeData>(
                    schemaString,
                    options);

            if (typeData == null)
            {
                throw new ArgumentException(
                    $"Incorrect format:  '{schemaString}'",
                    nameof(schemaString));
            }
            if (!string.Equals(typeData.Type, "struct", StringComparison.OrdinalIgnoreCase))
            {
                throw new MirrorException($"schemaString type isn't a struct:  '{schemaString}'");
            }
            if (typeData.Fields == null)
            {
                throw new MirrorException($"schemaString doesn't contain fields:  '{schemaString}'");
            }

            var schema = typeData
                .Fields
                .Select(f => new ColumnDefinition
                {
                    ColumnName = f.Name,
                    ColumnType = GetKustoType(f.Type.ToLower())
                })
                .ToImmutableArray();

            return schema;
        }

        private static string GetKustoType(string type)
        {
            switch (type)
            {
                case "string":
                case "long":
                case "double":
                case "boolean":
                case "decimal":
                    return type;
                case "integer":
                case "short":
                case "byte":
                    return "int";
                case "float":
                    return "real";
                case "binary":
                    return "Parquet binary field type isn't supported in Kusto";
                case "date":
                case "timestamp":
                    return "datetime";
                case "":
                    return "dynamic";

                default:
                    throw new NotImplementedException($"Unsupported field type:  '{type}'");
            }
        }
        private static long ExtractRecordCount(string stats)
        {
            var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
            var statsData =
                JsonSerializer.Deserialize<TransactionLogEntry.AddData.StatsData>(
                    stats,
                    options);

            if (statsData == null)
            {
                throw new ArgumentException(
                    $"Incorrect format:  '{stats}'",
                    nameof(stats));
            }

            return statsData.NumRecords;
        }
        #endregion

        public MetadataData? Metadata { get; set; }

        public AddData? Add { get; set; }

        public RemoveData? Remove { get; set; }

        public TxnData? Txn { get; set; }

        public ProtocolData? Protocol { get; set; }

        public CommitInfoData? CommitInfo { get; set; }
    }
}