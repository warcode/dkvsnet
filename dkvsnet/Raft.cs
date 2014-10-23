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
        int heartbeatWaitPeriod = 50;
        int electionWaitPeriod = 150;
        DateTime? electionTimeout;
        DateTime? heartbeatTimeout;
        BlockingCollection<RaftMessage> outputQueue;
        BlockingCollection<RaftMessage> inputQueue;
        RaftState state = RaftState.Follower;
        string votedFor = null;
        private static RNGCryptoServiceProvider rng = new RNGCryptoServiceProvider();
        private Dictionary<string, RaftNode> remoteNodes = new Dictionary<string, RaftNode>();
        int term = 0;


        public Raft(string host, string nodefile, BlockingCollection<RaftMessage> input, BlockingCollection<RaftMessage> output)
        {
            _host = host;
            inputQueue = input;
            outputQueue = output;

            byte[] randomNumber = new byte[2];
            rng.GetNonZeroBytes(randomNumber);

            var number = Math.Abs(BitConverter.ToInt16(randomNumber, 0) % 100);
            electionWaitPeriod = 1500 + ((int)Math.Floor(1.5 * (number)));

            LoadPresetNodes(nodefile);
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
            switch (type)
            {
                case RaftMessageType.Vote:
                    break;
                case RaftMessageType.RequestVote:
                    break;
                case RaftMessageType.AppendEntries:
                    break;
            }

            var msg = new RaftMessage
            {
                Type = type,
                Destination = destination,
                Data = data
            };

            outputQueue.Add(msg);
        }


        private void ProcessMessage(RaftMessage message)
        {
            switch (message.Type)
            {
                case RaftMessageType.Vote:
                    VoteReceived(message.Sender);
                    break;
                case RaftMessageType.RequestVote:
                    if (Int32.Parse(message.Data) > term)
                    {
                        Vote(message.Sender, message.Data);
                    }
                    break;
                case RaftMessageType.AppendEntries:
                    AppendEntries();
                    break;
            }
        }

        private void VoteReceived(string from)
        {
            if (state != RaftState.Leader)
            {
                Console.Out.WriteLine("Got vote from " + from);

                remoteNodes[from].VoteGranted = true;
                //me + others = total / 2 + 1 = quora
                var votesForMe = 1 + remoteNodes.Values.Where(x => x.VoteGranted).Count();
                var quoraNeeded = Math.Floor((double)((1 + remoteNodes.Keys.Count())) / 2) + 1;

                Console.Out.WriteLine("I need " + quoraNeeded + " votes. I have " + votesForMe + " votes");

                if (votesForMe >= quoraNeeded)
                {
                    BecomeLeader();
                }
            }
        }

        //Raft Actions
        void BecomeCandidate()
        {
            Console.Out.WriteLine("Becoming candidate");
            //Deal with yourself
            state = RaftState.Candidate;
            electionTimeout = DateTime.UtcNow.AddMilliseconds(electionWaitPeriod);
            term = term + 1;
            votedFor = _host;

            Console.Out.WriteLine("Requesting votes");
            foreach(var node in remoteNodes.Keys)
            {
                remoteNodes[node].VoteGranted = false;
                Send(RaftMessageType.RequestVote, node, term.ToString());
            }
        }

        private void BecomeLeader()
        {
            Console.Out.WriteLine("Look at me. LOOK AT ME. I'm da captain now.");
            state = RaftState.Leader;
            heartbeatWaitPeriod = electionWaitPeriod - 1000;
            heartbeatTimeout = DateTime.UtcNow.AddMilliseconds(heartbeatWaitPeriod);
        }

        void AppendEntries()
        {
            Console.Out.WriteLine("Yes boss.");
            state = RaftState.Follower;
            electionTimeout = DateTime.UtcNow.AddMilliseconds(electionWaitPeriod);
        }

        void Vote(string requester,  string data)
        {
            Send(RaftMessageType.Vote, requester, data);
            votedFor = requester;
        }

        void SendHeartbeat()
        {
            foreach (var node in remoteNodes.Keys)
            {
                Send(RaftMessageType.AppendEntries, node, term + "|");
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
}
