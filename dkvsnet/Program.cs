using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Owin.Hosting;
using Microsoft.Owin;
using Owin;
using ZeroMQ;
using System.Threading;

namespace dkvsnet
{
    class Program
    {

        static string value = "";
        static ZmqContext context;
        static string remote;


        static void Main(string[] args)
        {
            using (WebApp.Start<Startup>("http://localhost:" + (Int32.Parse(args[0]) + 1000)))
            {
                var localport = args[0];

                var com = new UdpComms(Int32.Parse(localport));
                com.StartIncoming();
                com.StartOutgoing();

                var raft = new Raft(com.LocalHost, @"X:\code\repo\cluster\cluster.cfg", com.incoming, com.outgoing);
                raft.StartListening();

                

                while (true)
                {
                    raft.Timeout();
                    Thread.Sleep(10);
                }
            }
        }
    }



    public class Startup
    {
        public void Configuration(IAppBuilder app)
        {

            #if DEBUG
            app.UseErrorPage();
            #endif
            
            app.UseWelcomePage("/");

            app.Run(context =>
            {
                if (context.Request.Path == new PathString("/test"))
                {
                    return context.Response.WriteAsync("Hello, world");
                }

                if (context.Request.Path == new PathString("/get"))
                {
                    return context.Response.WriteAsync("get");
                }

                if (context.Request.Path == new PathString("/set"))
                {
                    return context.Response.WriteAsync("set");
                }

                return context.Response.WriteAsync("what");
            });
        }
    }
}
