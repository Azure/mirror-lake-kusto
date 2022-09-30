using System.Runtime.Serialization;

namespace MirrorLakeKusto
{
    [Serializable]
    internal class MirrorException : Exception
    {
        public MirrorException(string message, Exception? innerException = null)
            : base(message, innerException)
        {
        }
    }
}