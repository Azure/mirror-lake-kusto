using CsvHelper.Configuration.Attributes;

namespace MirrorLakeKusto.Storage
{
    public class SchemaInternalState
    {
        /// <summary>Unique id of the delta table (in Spark).</summary>
        [Index(1500)]
        public Guid? DeltaTableId { get; set; }

        /// <summary>Unique id of the delta table (in Spark).</summary>
        [Index(1600)]
        public string? DeltaTableName { get; set; }
    }
}