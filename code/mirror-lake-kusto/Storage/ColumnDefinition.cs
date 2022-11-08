using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MirrorLakeKusto.Storage
{
    internal class ColumnDefinition
    {
        public string ColumnName { get; set; } = "UNDEFINED NAME";
        
        public string ColumnType { get; set; } = "UNDEFINED TYPE";

        #region Object Methods
        public override bool Equals(object? obj)
        {
            var other = obj as ColumnDefinition;

            return other != null
                && other.ColumnName == ColumnName
                && other.ColumnType == ColumnType;
        }

        public override int GetHashCode()
        {
            return ColumnName.GetHashCode() ^ ColumnType.GetHashCode();
        }
        #endregion
    }
}