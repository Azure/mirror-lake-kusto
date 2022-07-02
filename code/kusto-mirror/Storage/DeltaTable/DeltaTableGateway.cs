using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kusto.Mirror.ConsoleApp.Storage.DeltaTable
{
    internal class DeltaTableGateway
    {
        private Uri deltaTableStorageUrl;

        public DeltaTableGateway(Uri deltaTableStorageUrl)
        {
            this.deltaTableStorageUrl = deltaTableStorageUrl;
        }
    }
}