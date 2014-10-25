using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ZeroMQ;
using System.Threading;
using System.Reactive;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace dkvsnet
{
    class ZmqComms
    {
        public ZmqContext context;
        private int listenport;
        public BlockingCollection<RaftMessage> incoming;
        private ConcurrentQueue<RaftMessage> inQueue;
        public BlockingCollection<RaftMessage> outgoing;
        private ConcurrentQueue<RaftMessage> outQueue;
        public string LocalHost;


        public ZmqComms(int port)
        {
            listenport = port;
            inQueue = new ConcurrentQueue<RaftMessage>();
            incoming = new BlockingCollection<RaftMessage>(inQueue);
            outQueue = new ConcurrentQueue<RaftMessage>();
            outgoing = new BlockingCollection<RaftMessage>(outQueue);
        }

        public void StartIncoming()
        {
            // ZMQ Context, server socket
            context = ZmqContext.Create();
            LocalHost = LocalIPAddress() +":"+ listenport;

            Task.Factory.StartNew(() =>
            {
                string message;
                try
                {
                    ZmqSocket server = context.CreateSocket(ZeroMQ.SocketType.REP);
                    server.Bind("tcp://"+ LocalHost);

                    Console.Out.WriteLine("I'm listening on " + LocalHost);

                    while (true)
                    {
                        message = server.Receive(Encoding.Unicode);
                        if (message.Equals("OVER")) { continue; }

                        Console.WriteLine("COM: Got message {0}", message);
                        var dataParts = message.Split('|');
                        if(dataParts.Count() < 2)
                        {
                            continue;
                        }
                        var type = (RaftMessageType)Int32.Parse(dataParts[0]);
                        var sender = dataParts[1];

                        var data = (dataParts.Count() > 2 ? dataParts[2] : "");

                        var msg = new RaftMessage
                        {
                            Type = type,
                            Sender = sender,
                            Data = data
                        };

                        incoming.Add(msg);

                        server.Send("OVER", Encoding.UTF8);
                    }
                }
                catch (Exception ex)
                {
                    Console.Out.Write(ex);
                }
            }, TaskCreationOptions.LongRunning);
        }

        public void StartOutgoing()
        {
            Task.Factory.StartNew(() =>
            {
                foreach (var message in outgoing.GetConsumingEnumerable())
                {
                    using(var client = context.CreateSocket(ZeroMQ.SocketType.REQ))
                    {
                        Console.Out.WriteLine("COM: Sending "+ message.Type +" to " + message.Destination);
                        client.Connect("tcp://" + message.Destination);
                        client.Send(((int)message.Type) + "|" + LocalHost + "|" + message.Data, Encoding.Unicode);
                        var res = client.Receive(Encoding.Unicode);
                    }
                }
            }, TaskCreationOptions.LongRunning);
        }

        public string LocalIPAddress()
        {
            IPHostEntry host;
            string localIP = "";
            host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (IPAddress ip in host.AddressList)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    localIP = ip.ToString();
                    break;
                }
            }
            return localIP;
        }
    }
}
