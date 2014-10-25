using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace dkvsnet
{

    /// <summary>
    /// Raft will use the blocking collections as message queues for communication.
    /// Incoming messages should be added to the incoming collection and are handled by raft.
    /// Outgoing messages are added to the outgoing collection by raft and should be delivered by the comms package.
    /// </summary>
    interface IComms
    {

        BlockingCollection<RaftMessage> incoming { get; set; }
        BlockingCollection<RaftMessage> outgoing { get; set; }

        BlockingCollection<RaftMessage> StartIncoming();
        BlockingCollection<RaftMessage> StartOutgoing();

        string GetLocalHost();
    }
}
