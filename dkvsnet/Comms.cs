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
    class Comms
    {
        public ZmqContext context;
        private int listenport;
        public BlockingCollection<RaftMessage> append;
        private ConcurrentQueue<RaftMessage> queue;
        public string LocalHost;


        public Comms(int port)
        {
            listenport = port;
            queue = new ConcurrentQueue<RaftMessage>();
            append = new BlockingCollection<RaftMessage>(queue);
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

                    while ((message = server.Receive(Encoding.Unicode)) != null)
                    {
                        Console.WriteLine("Received request: {0}", message);
                        var dataParts = message.Split('|');
                        if(dataParts.Count() < 2)
                        {
                            continue;
                        }
                        var type = dataParts[0];
                        var sender = dataParts[1];

                        var data = (dataParts.Count() > 2 ? dataParts[2] : "");

                        var msg = new RaftMessage
                        {
                            Type = type,
                            Sender = sender,
                            Data = data
                        };

                        append.Add(msg);

                        server.Send("TRUE", Encoding.UTF8);
                    }
                }
                catch (Exception ex)
                {
                    Console.Out.Write(ex);
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
