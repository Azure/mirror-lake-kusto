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
        public TableDefinition(string tableName, IEnumerable<ColumnDefinition> columns)
        {
            Name = tableName;
            Columns = columns.ToImmutableArray();
        }

        public string Name { get; }
        
        public IImmutableList<ColumnDefinition> Columns { get; }
    }
}