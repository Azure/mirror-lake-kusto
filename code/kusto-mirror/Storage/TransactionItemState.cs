using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kusto.Mirror.ConsoleApp.Storage
{
    internal enum TransactionItemState
    {
        ToBeAdded,
        QueuedForIngestion,
        Staged,
        Loaded,
        ToBeDeleted,
        Deleted,
        ToBeApplied,
        Applied
    }
}