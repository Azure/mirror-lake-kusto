using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kusto.Mirror.ConsoleApp.Storage
{
    internal enum TransactionItemState
    {
        Initial,
        Done,
        QueuedForIngestion,
        Staged,
        Loaded
    }
}