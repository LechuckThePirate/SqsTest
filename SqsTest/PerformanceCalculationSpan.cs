using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqsTest
{
    public class PerformanceCalculationSpan
    {
        public int PackageNumber { get; set; }
        public DateTime LastDate { get; set; }
        public int FromAccount { get; set; }
        public int ToAccount { get; set; }
    }
}
