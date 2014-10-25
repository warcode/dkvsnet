using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace dkvsnet
{
    public class RaftMessage
    {
        public RaftMessageType Type { get; set; }
        public string Sender { get; set; }
        public string Destination { get; set; }
        public string Data { get; set; }
        public int Term { get; set; }
    }

    public enum RaftMessageType
    {
        RequestVote,
        Vote,
        AppendEntries
    }
}
