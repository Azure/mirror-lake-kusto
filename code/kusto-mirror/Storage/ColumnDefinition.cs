using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kusto.Mirror.ConsoleApp.Storage
{
    internal class ColumnDefinition
    {
        public string ColumnName { get; set; } = "UNDEFINED NAME";
        
        public string ColumnType { get; set; } = "UNDEFINED TYPE";
    }
}