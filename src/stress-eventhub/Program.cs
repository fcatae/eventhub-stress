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

        static string[] g_AllMessages = { "hello_world", "json_rules", "compact_binary" };

        public static void Main(string[] args)
        {
            Stopwatch watch = new Stopwatch();
            string message = g_AllMessages[0];
            int batchSize = 1;
            int asyncCount = 30;
            int loopCount = 10;
            int numberPublishers = 1;

            Console.WriteLine("Hello world");

            var builder = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .AddJsonFile("app.json", optional: true)
                .AddUserSecrets("aspnet-stress-eventhub-20160705064558")
                .AddCommandLine(args);

            var config = builder.Build();
            
            string proto = config["proto"] ?? "Amqp";
            string connectionString = config["eventhub-ns"];

            if(proto != null)
            {
                connectionString += $";TransportType={proto}";
            }

            if (config["batch"] != null)
            {
                batchSize = Int32.Parse(config["batch"]);
            }
            if (config["async"] != null)
            {
                asyncCount = Int32.Parse(config["async"]);
            }
            if (config["loop"] != null)
            {
                loopCount = Int32.Parse(config["loop"]);
            }
            if (config["pub"] != null)
            {
                numberPublishers = Int32.Parse(config["pub"]);
            }

            bool shouldReuse = (config["reuse"] != null);
            string eventhubPath = config["eventhub-path"];

            IPublisher pub = (numberPublishers == 1) ?
                (IPublisher)new Publisher(connectionString, eventhubPath, newConnection: false) : 
                (IPublisher)new MultiPublisher(numberPublishers, connectionString, eventhubPath, createNew: !shouldReuse);

            Console.WriteLine($"Using protocol: {proto}");

            Console.Write("Initializing... ");

            pub.InitAsync("Init").Wait();
            Console.WriteLine("DONE");

            watch.Start();

            Console.WriteLine($"Sending messages ({message})");
            Console.WriteLine($"  - loopCount = {loopCount}");
            Console.WriteLine($"  - asyncCount = {asyncCount}");
            Console.WriteLine($"  - batchSize = {batchSize}");
            SendAsync(pub, message, loopCount, asyncCount, batchSize).Wait();

            watch.Stop();

            Console.WriteLine();
            Console.WriteLine("  Total time = {0} ms", watch.ElapsedMilliseconds);
            Console.WriteLine("  Total messages= {0} msgs", Program.TotalMessages);
            Console.WriteLine();
            Console.WriteLine("  Througput= {0} msg/sec", 1000 * Program.TotalMessages / watch.ElapsedMilliseconds);

            //Console.ReadLine();
        }

        static async Task SendAsync(IPublisher pub, string message, int loopCount, int asyncCount, int batchSize)
        {
            Console.Write("Sending messages ({1}x {0}), batch size={2} .", message, asyncCount, batchSize);

            await pub.SendAsync(message, asyncCount, batchSize);

            Console.Write(".. ");

            for(int i=1; i<loopCount; i++)
            {
                await pub.SendAsync(message, asyncCount, batchSize);
            }

            Console.WriteLine("DONE");
        }
    }
}
