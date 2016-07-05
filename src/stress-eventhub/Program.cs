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
        public static int TotalMessages = 0;

        public static void Main(string[] args)
        {
            Stopwatch watch = new Stopwatch();
            string message = "Hello from Git";
            int batchSize = 10;
            int loopCount = 5;

            Console.WriteLine("Hello world");

            var builder = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .AddJsonFile("app.json", optional: true)
                .AddUserSecrets("aspnet-stress-eventhub-20160705064558")
                .AddCommandLine(args);

            var config = builder.Build();

            Publisher pub = new Publisher(config["eventhub"]);

            if (config["batch"] != null)
            {
                batchSize = Int32.Parse(config["batch"]);
            }
            if (config["loop"] != null)
            {
                loopCount = Int32.Parse(config["loop"]);
            }

            Console.Write("Initializing... ");

            pub.InitAsync().Wait();
            Console.WriteLine("DONE");

            watch.Start();

            Console.Write("Sending messages ({1}x {0}), batch size={2} .", message, loopCount, batchSize);
            var task = pub.SendAsync(message, loopCount, batchSize);

            Console.Write(".. ");
            task.Wait();

            Console.WriteLine("DONE");

            watch.Stop();

            Console.WriteLine();
            Console.WriteLine("  Total time = {0}", watch.ElapsedMilliseconds);
            Console.WriteLine("  Total messages= {0}", Program.TotalMessages);
            Console.WriteLine();
            Console.WriteLine("  Througput= {0}", 1000 * Program.TotalMessages / watch.ElapsedMilliseconds);

            Console.ReadLine();
        }
    }
}
