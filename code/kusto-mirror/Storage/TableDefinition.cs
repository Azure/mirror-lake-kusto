using Kusto.Data.Common;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kusto.Mirror.ConsoleApp.Storage
{
    internal class TableDefinition
    {
        private const string BLOB_PATH_COLUMN = "KM_BlobPath";
        private const string BLOB_ROW_NUMBER_COLUMN = "KM_Blob_RowNumber";

        public TableDefinition(string tableName, IEnumerable<ColumnDefinition> columns)
        {
            Name = tableName;
            Columns = columns.ToImmutableArray();
        }

        public string Name { get; }
        
        public IImmutableList<ColumnDefinition> Columns { get; }

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
                });

            return new TableDefinition(Name, moreColumns);
        }

        public TableDefinition RenameTable(string tableName)
        {
            return new TableDefinition(tableName, Columns);
        }

        public ImmutableArray<ColumnMapping> CreateIngestionMappings()
        {
            var location =
                new Dictionary<string, string>() { { "Transform", "SourceLocation" } };
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
                    : new Dictionary<string, string>()
                })
                .ToImmutableArray();

            return ingestionMappings;
        }

    }
}