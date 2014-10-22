using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace dkvsnet
{
    public class RaftMessage
    {
        public string Type { get; set; }
        public string Sender { get; set; }
        public string Data { get; set; }
    }
}
