﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MirrorLakeKusto.Storage
{
    internal enum TransactionItemState
    {
        Initial,
        Analyzed,
        Skipped,
        Done,
        QueuedForIngestion,
        Staged
    }
}