using Microsoft.Azure.Management.Kusto.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MirrorLakeKustoTest.Electric
{
    public class ElectricTestBase : TestBase
    {
        protected string CreationTimeExpression => "todatetime(strcat(p0,'-01-01'))";
    }
}