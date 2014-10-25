using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace dkvsnet
{
    class UdpComms : IComms
    {
        private int listenport;
        public BlockingCollection<RaftMessage> incoming { get; set; }
        public BlockingCollection<RaftMessage> outgoing { get; set; }
        private ConcurrentQueue<RaftMessage> inQueue;
        private ConcurrentQueue<RaftMessage> outQueue;
        string LocalHost { get;set; }


        public UdpComms(int port)
        {
            listenport = port;
            inQueue = new ConcurrentQueue<RaftMessage>();
            incoming = new BlockingCollection<RaftMessage>(inQueue);
            outQueue = new ConcurrentQueue<RaftMessage>();
            outgoing = new BlockingCollection<RaftMessage>(outQueue);
        }

        public BlockingCollection<RaftMessage> StartIncoming()
        {
            LocalHost = LocalIPAddress().ToString() + ":" + listenport;

            Task.Factory.StartNew(() =>
            {
                string message;
                try
                {
                    IPEndPoint localEndpoint = new IPEndPoint(LocalIPAddress(), listenport);
                    IPEndPoint remoteEndpoint = new IPEndPoint(IPAddress.Any, 0);

                    UdpClient server = new UdpClient(localEndpoint);

                    Console.Out.WriteLine("I'm listening on " + LocalHost);

                    while (true)
                    {
                        message = Encoding.UTF8.GetString(server.Receive(ref remoteEndpoint));

                        Console.WriteLine("COM: Got message {0}", message);
                        var dataParts = message.Split('|');
                        if (dataParts.Count() < 2)
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
                    }
                }
                catch (Exception ex)
                {
                    Console.Out.Write(ex);
                }
            }, TaskCreationOptions.LongRunning);

            return incoming;
        }

        public BlockingCollection<RaftMessage> StartOutgoing()
        {
            Task.Factory.StartNew(() =>
            {
                foreach (var message in outgoing.GetConsumingEnumerable())
                {
                    var destination = message.Destination.Split(':');

                    using (var client = new UdpClient(destination[0], Int32.Parse(destination[1])))
                    {
                        Console.Out.WriteLine("COM: Sending " + message.Type + " to " + message.Destination);
                        var messageData = Encoding.UTF8.GetBytes(((int)message.Type) + "|" + LocalHost + "|" + message.Data);
                        client.Send( messageData, messageData.Length  );
                    }
                }
            }, TaskCreationOptions.LongRunning);

            return outgoing;
        }

        public IPAddress LocalIPAddress()
        {
            IPHostEntry host;
            host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (IPAddress ip in host.AddressList)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    return ip;
                }
            }

            return null;
        }

        public string GetLocalHost()
        {
            if(string.IsNullOrEmpty(LocalHost))
            {
                throw new Exception("You must start the comms package before attempting to get the local host.");
            }

            return LocalHost;
        }
    }
}
