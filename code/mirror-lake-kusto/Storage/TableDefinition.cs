using Kusto.Data.Common;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MirrorLakeKusto.Storage
{
    internal class TableDefinition
    {
        private static readonly Dictionary<string, string> EMPTY_MAP =
            new Dictionary<string, string>();

        private const string BLOB_PATH_COLUMN = "KM_BlobPath";
        private const string BLOB_ROW_NUMBER_COLUMN = "KM_Blob_RowNumber";

        #region Constructors
        public TableDefinition(
            string tableName,
            IEnumerable<ColumnDefinition> columns,
            IEnumerable<string> partitionColumns)
            : this(tableName, columns.ToImmutableArray(), partitionColumns.ToImmutableArray())
        {
        }

        private TableDefinition(
            string tableName,
            IImmutableList<ColumnDefinition> columns,
            IImmutableList<string> partitionColumns)
        {
            Name = tableName;
            Columns = columns;
            PartitionColumns = partitionColumns;
        }
        #endregion

        public string Name { get; }

        public string BlobPathColumnName => BLOB_PATH_COLUMN;

        public IImmutableList<ColumnDefinition> Columns { get; }

        public IImmutableList<string> PartitionColumns { get; }

        public string KustoSchema
        {
            get
            {
                var columnsText = Columns
                    .Select(c => $"['{c.ColumnName}']:{c.ColumnType}");
                var schemaText = string.Join(", ", columnsText);

                return schemaText;
            }
        }

        public TableDefinition WithTrackingColumns()
        {
            var moreColumns = Columns
                .Append(new ColumnDefinition
                {
                    ColumnName = BLOB_PATH_COLUMN,
                    ColumnType = "string"
                })
                .Append(new ColumnDefinition
                {
                    ColumnName = BLOB_ROW_NUMBER_COLUMN,
                    ColumnType = "long"
                })
                .ToImmutableArray();

            return new TableDefinition(Name, moreColumns, PartitionColumns);
        }

        public TableDefinition RenameTable(string tableName)
        {
            return new TableDefinition(tableName, Columns, PartitionColumns);
        }

        public ImmutableArray<ColumnMapping> CreateIngestionMappings(
            string blobPath,
            IImmutableDictionary<string, string> partitionValues)
        {
            var location =
                new Dictionary<string, string>() { { "ConstValue", blobPath } };
            var lineNumber =
                new Dictionary<string, string>() { { "Transform", "SourceLineNumber" } };
            var ingestionMappings = Columns
                .Select(c => new ColumnMapping()
                {
                    ColumnName = c.ColumnName,
                    ColumnType = c.ColumnType,
                    Properties = c.ColumnName == BLOB_PATH_COLUMN
                    ? location
                    : c.ColumnName == BLOB_ROW_NUMBER_COLUMN
                    ? lineNumber
                    : partitionValues.ContainsKey(c.ColumnName)
                    ? new Dictionary<string, string>() {
                        { "ConstValue", partitionValues[c.ColumnName] } }
                    : EMPTY_MAP
                })
                .ToImmutableArray();

            return ingestionMappings;
        }

    }
}