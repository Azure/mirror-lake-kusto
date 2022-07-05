using Kusto.Cloud.Platform.Utils;
using System.Collections.Immutable;
using System.Text.Json;

namespace Kusto.Mirror.ConsoleApp.Storage.DeltaTable
{
    internal class TransactionMetadata
    {
        public TransactionMetadata(TransactionLogEntry.MetadataData metadata)
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
            DeltaTableId = metadata.Id;
            DeltaTableName = metadata.Name;
            PartitionColumns = metadata.PartitionColumns == null
                ? ImmutableArray<string>.Empty
                : metadata.PartitionColumns.ToImmutableArray();
            Schema = ExtractSchema(metadata.SchemaString);
            CreatedTime = DateTimeOffset
                .FromUnixTimeMilliseconds(metadata.CreatedTime)
                .UtcDateTime;
        }

        public Guid DeltaTableId { get; }

        public string DeltaTableName { get; }

        public IImmutableList<string> PartitionColumns { get; }

        public IImmutableDictionary<string, string> Schema { get; set; }

        public DateTime CreatedTime { get; }

        private static IImmutableDictionary<string, string> ExtractSchema(string schemaString)
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
                .ToImmutableDictionary(f => f.Name, f => GetKustoType(f.Type.ToLower()));

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
    }
}