using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.ServiceBus.Messaging;

namespace stress_eventhub
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Stopwatch watch = new Stopwatch();
            string message = "Hello from Git";
            int batchSize = 10;

            Console.WriteLine("Hello world");

            var builder = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .AddUserSecrets("aspnet-stress-eventhub-20160705064558")
                .AddCommandLine(args);

            var config = builder.Build();

            Publisher pub = new Publisher(config["eventhub"]);

            if(config["batch"] != null)
            {
                batchSize = Int32.Parse(config["batch"]);
            }

            Console.Write("Initializing... ");

            pub.InitAsync().Wait();
            Console.WriteLine("DONE");

            watch.Start();

            Console.Write("Sending messages ({1}x {0})... ", message, batchSize);
            pub.SendAsync(message, batchSize).Wait();
            Console.WriteLine("DONE");

            watch.Stop();

            Console.WriteLine();
            Console.WriteLine("  Total time = {0}", watch.ElapsedMilliseconds);

            Console.ReadLine();
        }
    }
}
