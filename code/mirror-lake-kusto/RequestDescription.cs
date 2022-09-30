using System.Text.Json.Serialization;

namespace MirrorLakeKusto
{
    internal class RequestDescription
    {
        public string? SessionId { get; set; }

        public string? Os { get; set; }

        public string? OsVersion { get; set; }

        public string? AuthenticationMode { get; set; }

        public List<RequestDescriptionTable>? Tables { get; set; }
    }

    [JsonSerializable(typeof(RequestDescription))]
    internal partial class RequestDescriptionSerializerContext : JsonSerializerContext
    {
    }
}