using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using ZeroMQ;

namespace dkvsnet
{
    class Raft
    {
        string _host;
        private Dictionary<string, RaftNode> remoteNodes = new Dictionary<string, RaftNode>();
        private static RNGCryptoServiceProvider rng = new RNGCryptoServiceProvider();

        int heartbeatWaitPeriod = 50;
        int electionWaitPeriod = 150;
        DateTime? electionTimeout;
        DateTime? heartbeatTimeout;

        BlockingCollection<RaftMessage> outputQueue;
        BlockingCollection<RaftMessage> inputQueue;

        string leader;
        RaftState state = RaftState.Follower;
        string votedFor = null;
        int currentTerm = 0;
        List<RaftLogEntry> log = new List<RaftLogEntry>();
        

        int commitIndex = 0;
        int lastApplied = 0;


        public Raft(string nodefile, IComms comms)
        {
            inputQueue = comms.StartIncoming();
            outputQueue = comms.StartOutgoing();
            _host = comms.GetLocalHost();

            electionWaitPeriod = 1500 + GetRandomWaitPeriod();

            LoadPresetNodes(nodefile);
        }

        private int GetRandomWaitPeriod()
        {
            byte[] randomNumber = new byte[2];
            rng.GetNonZeroBytes(randomNumber);

            var number = Math.Abs(BitConverter.ToInt16(randomNumber, 0) % 100);
            return ((int)Math.Floor(1.5 * (number)));
        }


        private void LoadPresetNodes(string location)
        {
            var nodelist = File.ReadAllLines(location);
            foreach(var node in nodelist)
            {
                if(node.Equals(_host)) { continue; }

                var raftNode = new RaftNode
                {
                    Identifier = node,
                    MatchIndex = 0,
                    NextIndex = 1,
                    VoteGranted = false
                };

                remoteNodes.Add(node, raftNode);
            }
        }


        public void StartListening()
        {
            Task.Factory.StartNew(() =>
            {
                foreach (var message in inputQueue.GetConsumingEnumerable())
                {
                    ProcessMessage(message);
                }
            }, TaskCreationOptions.LongRunning);
        }


        private void Send(RaftMessageType type, string destination, string data)
        {
            var msg = new RaftMessage
            {
                Type = type,
                Term = currentTerm,
                Destination = destination,
                Data = data
            };

            outputQueue.Add(msg);
        }


        private void ProcessMessage(RaftMessage message)
        {
            //If any message has a bigger term, set term = message.term and become follower
            if(message.Term > currentTerm)
            {
                state = RaftState.Follower;
                currentTerm = message.Term;
            }

            switch (message.Type)
            {
                case RaftMessageType.Vote:
                    VoteReceived(message);
                    break;
                case RaftMessageType.RequestVote:
                    Vote(message);
                    break;
                case RaftMessageType.AppendEntries:
                    AppendEntries(message);
                    break;
            }
        }

        private void VoteReceived(RaftMessage message)
        {
            if (message.Data == "1")
            {
                if (state != RaftState.Leader)
                {
                    var from = message.Sender;
                    Console.Out.WriteLine("Got vote from " + from);

                    remoteNodes[from].VoteGranted = true;
                    //me + others = total / 2 + 1 = quora
                    var votesForMe = 1 + remoteNodes.Values.Where(x => x.VoteGranted).Count();
                    var votesNeeded = Math.Floor((double)((1 + remoteNodes.Keys.Count())) / 2) + 1;

                    Console.Out.WriteLine("I need " + votesNeeded + " votes. I have " + votesForMe + " votes");

                    if (votesForMe >= votesNeeded)
                    {
                        BecomeLeader();
                    }
                }
            }
        }

        //Raft Actions
        void BecomeCandidate()
        {
            Console.Out.WriteLine("Becoming candidate");
            //Deal with yourself
            state = RaftState.Candidate;
            currentTerm = currentTerm + 1;
            votedFor = _host;

            electionWaitPeriod = 1500 + GetRandomWaitPeriod();
            electionTimeout = DateTime.UtcNow.AddMilliseconds(electionWaitPeriod);

            Console.Out.WriteLine("Requesting votes");
            foreach(var node in remoteNodes.Keys)
            {
                remoteNodes[node].VoteGranted = false;
                Send(RaftMessageType.RequestVote, node, (lastApplied > 0 ? lastApplied + "-" + log[lastApplied].Term : "0-0"));
            }
        }

        private void BecomeLeader()
        {
            Console.Out.WriteLine("I AM THE LEADER");
            state = RaftState.Leader;
            heartbeatWaitPeriod = electionWaitPeriod - 1000;
            heartbeatTimeout = DateTime.UtcNow;

            foreach (var node in remoteNodes.Keys)
            {
                remoteNodes[node].MatchIndex = 0;
                remoteNodes[node].NextIndex = commitIndex;
            }

        }

        void AppendEntries(RaftMessage message)
        {
            //Ignore if message.term < term
            if(message.Term < currentTerm)
            {
                Console.Out.WriteLine("Old term detected. Discarding append entries.");
                return;
            }

            if (string.IsNullOrEmpty(message.Data))
            {
                Console.Out.WriteLine("FOLLOWING ORDERS");
                leader = message.Sender;
                state = RaftState.Follower;
                votedFor = null;
                electionTimeout = DateTime.UtcNow.AddMilliseconds(electionWaitPeriod);
            }
            else
            {

                var data = message.Data.Split('-');
                var prevLogIndex = Int32.Parse(data[0]);
                var prevLogTerm = Int32.Parse(data[1]);
                var leaderCommitIndex = Int32.Parse(data[2]);

                //ignore if log doesn't contain an entry at prevLogIndex whos term is equal to prevLogTerm
                if(log[prevLogIndex] != null)
                {
                    if (log[prevLogIndex].Term != prevLogTerm)
                    {
                        Console.Out.WriteLine("Incorrect term for previous log entry detected. Discarding append entries.");
                        return;
                    }
                }

                Console.Out.WriteLine("DOING WORK");

                //If entry conflicts, delete entry and following entries
                /*
                if (log[RaftLogEntry.Index] != null && log[RaftLogEntry.Index].Term != RaftLogEntry.Term)
                {
                    var toBeDeleted = log.Where(x => x.Index >= RaftLogEntry.Index).Select(x => x.Index).ToList();
                    foreach(var index in toBeDeleted)
                    {
                        log[index] = null;
                    }
                }
                */

                //Append all new entries to log
                /*
                if(leaderCommitIndex > commitIndex)
                {
                    commitIndex = Math.Min(leaderCommitIndex, lastNewEntry);
                }
                */

            }

            



        }

        void Vote(RaftMessage message)
        {
            var requester = message.Sender;
            if(message.Term < currentTerm)
            {
                Send(RaftMessageType.Vote, requester, "0");
                return;
            }

            var logData = message.Data.Split('-');
            var lastLogIndex = Int32.Parse(logData[0]);
            var lastLogTerm = Int32.Parse(logData[1]);

            if (votedFor == null || (lastLogIndex >= lastApplied && (lastApplied == 0 ? true : log[lastApplied].Term >= lastLogTerm)))
            {
                
                Console.Out.WriteLine("I voted for " + requester);
                votedFor = requester;
                electionTimeout = DateTime.UtcNow.AddMilliseconds(electionWaitPeriod);
                Send(RaftMessageType.Vote, requester, "1");
            }

        }

        void SendHeartbeat()
        {
            foreach (var node in remoteNodes.Keys)
            {
                Send(RaftMessageType.AppendEntries, node, "");
            }
        }

        public void Timeout()
        {
            if(electionTimeout == null)
            {
                electionTimeout = DateTime.UtcNow.AddMilliseconds(electionWaitPeriod);
            }

            if (state != RaftState.Leader && DateTime.UtcNow >= electionTimeout)
            {
                BecomeCandidate();
            }

            if (state == RaftState.Leader && DateTime.UtcNow >= heartbeatTimeout)
            {
                heartbeatTimeout = DateTime.UtcNow.AddMilliseconds(heartbeatWaitPeriod);
                SendHeartbeat();
            }
        }

        private enum RaftState
        { 
            Follower,
            Candidate,
            Leader
        }
    }

    class RaftNode
    {
        public string Identifier { get; set; }
        public int NextIndex { get; set; }
        public int MatchIndex { get; set; }
        public bool VoteGranted { get; set; }
    }

    class RaftLogEntry
    {
        public int Index {get;set;}
        public int Term { get; set; }
        public string Entry { get; set; }
        public bool Committed { get; set; }
    }
}
