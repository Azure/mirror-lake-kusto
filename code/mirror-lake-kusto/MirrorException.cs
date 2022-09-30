using System.Runtime.Serialization;

namespace Kusto.Mirror.ConsoleApp
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